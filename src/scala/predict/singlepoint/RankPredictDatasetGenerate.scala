package predict.singlepoint

import javafx.scene.media.VideoTrack
import mam.{Dic, SparkSessionInit}
import mam.GetSaveData._
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getOrderHistorySimilarity, getPlayHistorySimilarity, printDf, sysParamSetting, udfGetLastVideo}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RankPredictDatasetGenerate {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val timeWindowStart=args(0)+" "+args(1)
    val timeWindowEnd=args(2)+" "+args(3)

    val userProfileOrderPart=getUserProfileOrderPart(spark,timeWindowStart,"predict")
    val userProfilePlayPart=getUserProfilePlayPart(spark,timeWindowStart,"predict")
    val userProfilePreferencePart=getUserProfilePreferencePart(spark,timeWindowStart,"predict")

    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)
    val userProfile=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
      .join(userProfileOrderPart,joinKeysUserId,"left")
    printDf("输入 userProfiles",userProfile)
    val videoProfile=getVideoProfile(spark,timeWindowStart,"predict")
    val videoVector=getVideoVector(spark,timeWindowStart,"predict")

    printDf("输入  videoProfile",videoProfile)
    printDf("输入  videoVector",videoVector)

    //获取用户划分阶段预测得到的可能购买单点视频的用户
    val userDivisionResult=getUserDivisionResult(spark,timeWindowStart,timeWindowEnd)

    printDf("输入  userDivisionResult",userDivisionResult)


    val userOrderList=getOrderList(spark,timeWindowStart,"predict")
    printDf("输入  userOrderList",userOrderList)


    //对用户的视频进行召回

    //1.w2v召回
    val df_medias=getProcessedMedias(spark)
    val recall_w2v=recallByW2V(userDivisionResult,videoVector,userOrderList,df_medias)

    //2.热门单点视频的召回
    val df_orders=getProcessedOrder(spark)
    val hotVideos=recallHotVideos(df_orders,10,timeWindowStart)
    val userHotVideos=userDivisionResult.select(col(Dic.colUserId)).crossJoin(hotVideos)

    //将召回结果进行合并去重
    val recommends=userHotVideos.union(recall_w2v).dropDuplicates()


    //添加订单历史相似度和播放历史相似度两个属性
    val orderHistory=getOrderList(spark,timeWindowStart,"predict")
    val playHistory=getPlayList(spark,timeWindowStart,"predict")
    val orderSim=getOrderHistorySimilarity(recommends,orderHistory,videoVector)
    val playSim=getPlayHistorySimilarity(recommends,playHistory,videoVector)

   //拼接上所有属性
    val result=recommends.join(userProfile,joinKeysUserId,"left")
      .join(videoProfile,joinKeysVideoId,"left")
      .join(orderSim,Seq(Dic.colUserId,Dic.colVideoId),"left")
      .join(playSim,Seq(Dic.colUserId,Dic.colVideoId),"left")

    //进行属性的过滤以及归一化等操作
    val df_result_scaled=processPredictSample(result)

    //Save Data

    printDf("输出  df_result_scaled",df_result_scaled)

    saveSinglePointPredictSamples(timeWindowStart,timeWindowEnd,df_result_scaled)
    println("RankPredictDatasetGenerate  over~~~~~~~~~~~")









  }
  def processPredictSample(result:DataFrame)={
    val colTypeList=result.dtypes.toList
    val colList=ArrayBuffer[String]()
    colList.append(Dic.colUserId)
    colList.append(Dic.colVideoId)
    for(elem<- colTypeList){
      if(elem._2.equals("IntegerType") || elem._2.equals("DoubleType") || elem._2.equals("LongType")){
        colList.append(elem._1)
      }
    }
    colList-=Dic.colIsSingle
    colList-=Dic.colIsTrailers
    colList-=Dic.colIsPaid
    val seqColList=colList.toSeq
    val predictSamples=result.select(seqColList.map(result.col(_)):_*)
      .na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)
    val exclude_cols = Array(Dic.colUserId,Dic.colVideoId)
    val df_result_scaled = scaleData(predictSamples, exclude_cols)
    df_result_scaled

  }

  //选出最近一个月内最为流行的num部单点视频
  def recallHotVideos(df_orders:DataFrame,num:Int,timeWindowStart:String)={
    val pre30=calDate(timeWindowStart,-30)
    df_orders.filter(
          col(Dic.colResourceType).===(0)
          && col(Dic.colCreationTime).<(timeWindowStart)
          && col(Dic.colCreationTime).>=(pre30)
          && col(Dic.colOrderStatus).>(1)
        ).groupBy(col(Dic.colResourceId))
          .agg(count(col(Dic.colUserId)).as("count"))
          .orderBy(col("count").desc)
          .limit(num)
          .select(col(Dic.colResourceId))
          .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)

  }


  def recallByW2V(userDivisionResult:DataFrame,videoVector:DataFrame,userOrderList:DataFrame,df_medias:DataFrame)={
    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)
    //得到用户最近购买的单点视频
    val userLastOrderVideo=userOrderList.withColumn(Dic.colVideoId,udfGetLastVideo(col(Dic.colOrderList)))
    //和videoVector结合，找到和当前单点视频最相似的5个单点视频,召回的数量取决于VideoVector中规定的每个视频的相似视频的数量
    val orderAndSim=videoVector.select(col(Dic.colVideoId),explode(col(Dic.colSimVideos)).as(Dic.colSimVideos))
    val recall_result=userLastOrderVideo.join(orderAndSim,joinKeysVideoId,"inner")
      .select(col(Dic.colUserId),col(Dic.colSimVideos))
      .withColumnRenamed(Dic.colSimVideos,Dic.colVideoId)
    //将召回的视频中不属于付费单点视频的筛出去
    val recall_result_filter=recall_result.join(df_medias.select(col(Dic.colVideoId),col(Dic.colIsSingle)),joinKeysVideoId,"inner")
      .filter(col(Dic.colIsSingle).===(1))
      .select(col(Dic.colUserId),col(Dic.colVideoId))
      .join(userDivisionResult,joinKeysUserId,"inner")
      .select(col(Dic.colUserId),col(Dic.colVideoId))

    recall_result_filter


  }
   //效果比较差，暂时不使用
  def  recallVideosGenerate(spark:SparkSession,playsPath:String,mediasPath:String): Unit ={

    val plays=spark.read.format("parquet").load(playsPath)
    val medias=spark.read.format("parquet").load(mediasPath)

    //得到用户对于单点视频的观看记录
    val joinKeysVideoId=Seq(Dic.colVideoId)
    val dataset=plays
      .join(medias.select(col(Dic.colVideoId),col(Dic.colIsSingle),col(Dic.colVideoTitle)),joinKeysVideoId,"inner")
      .filter(col(Dic.colIsSingle).===(1)
        && col(Dic.colBroadcastTime).>(360))
      .select(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colVideoTitle))
      .withColumn(Dic.colView,lit(1))
    dataset.show()
    //对user_id和video_id进行label_encode编码
    val userIDIndexer = new StringIndexer().setInputCol(Dic.colUserId).setOutputCol(Dic.colUserIdIndex)
    val videoIDIndexer = new StringIndexer().setInputCol(Dic.colVideoId).setOutputCol(Dic.colVideoIdIndex)
    val dataset_user_index=userIDIndexer.fit(dataset).transform(dataset)
    val dataset_indexed=videoIDIndexer.fit(dataset_user_index).transform(dataset_user_index)

    val df_user_index=dataset_indexed.select(col(Dic.colUserId),col(Dic.colUserIdIndex)).dropDuplicates().orderBy(col(Dic.colUserIdIndex))
    val df_video_index=dataset_indexed.select(col(Dic.colVideoId),col(Dic.colVideoIdIndex)).dropDuplicates().orderBy(col(Dic.colVideoIdIndex))

    df_user_index.show()
    df_video_index.show()

    //使用协同过滤算法
    val als = new ALS()    //定义一个ALS
      .setUserCol(Dic.colUserIdIndex)    //userid
      .setItemCol(Dic.colVideoIdIndex)    //itemid
      .setRatingCol(Dic.colView)    //rating矩阵，这里跟你输入的字段名字要保持一致。很明显这里是显示评分得到的矩阵形式
      .setMaxIter(5)
      .setRegParam(0.01)
      .setImplicitPrefs(true)//此处表明rating矩阵是隐式评分
    val model = als.fit(dataset_indexed)

    val joinKeyUserIdIndex=Seq(Dic.colUserIdIndex)
    val joinKeyVideoIdIndex=Seq(Dic.colVideoIdIndex)
    val df_recommendations=model.recommendForAllUsers(20)
    val df_rec=df_recommendations.select(col(Dic.colUserIdIndex),explode(col(Dic.colRecommendations)).as(Dic.colRecommendations))
      .withColumn(Dic.colVideoIdIndex,col(Dic.colRecommendations).getField(Dic.colVideoIdIndex))
      .withColumn(Dic.colRating,col(Dic.colRecommendations).getField(Dic.colRating))
      .join(df_user_index,joinKeyUserIdIndex,"inner")
      .join(df_video_index,joinKeyVideoIdIndex,"inner")
      .select(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colRating))
    //过滤掉用户已经观看过的单点视频
    val df_rec_except=df_rec.select(col(Dic.colUserId),col(Dic.colVideoId))
      .except(dataset.select(col(Dic.colUserId),col(Dic.colVideoId)).dropDuplicates(Seq(Dic.colUserId,Dic.colVideoId)))
      .join(df_rec,Seq(Dic.colUserId,Dic.colVideoId),"inner")



    df_rec_except.join(medias,joinKeysVideoId,"inner")
      .filter(col(Dic.colUserId).===(13095094))
      .select(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colRating),col(Dic.colVideoTitle))
      .show(40)
    dataset.filter(col(Dic.colUserId).===(13095094))
      .select(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colVideoTitle))
      .show(40)


  }




}
