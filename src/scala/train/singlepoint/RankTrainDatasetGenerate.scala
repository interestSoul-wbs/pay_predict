package train.singlepoint

import mam.{Dic, SparkSessionInit, Utils}
import mam.GetSaveData.{getOrderList, getPlayList, getProcessedOrder, getUserProfileOrderPart, getUserProfilePlayPart, getUserProfilePreferencePart, getVideoProfile, getVideoVector, saveSinglePointTrainSamples, scaleData}
import mam.SparkSessionInit.spark
import mam.Utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector

import scala.collection.mutable.ArrayBuffer

object RankTrainDatasetGenerate {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val timeWindowStart=args(0)+" "+args(1)
    val timeWindowEnd=args(2)+" "+args(3)
    //获取订单信息，用来构造正样本
    val df_orders=getProcessedOrder(spark)
    printDf("输入 df_order",df_orders)
    //获取用户画像
    val userProfileOrderPart=getUserProfileOrderPart(spark,timeWindowStart,"train")
    val userProfilePlayPart=getUserProfilePlayPart(spark,timeWindowStart,"train")
    val userProfilePreferencePart=getUserProfilePreferencePart(spark,timeWindowStart,"train")
    val joinKeysUserId=Seq(Dic.colUserId)
    val userProfile=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
      .join(userProfileOrderPart,joinKeysUserId,"left")
    printDf("输入 userProfiles",userProfile)
    //获取视频画像和视频的嵌入向量
    val videoProfile=getVideoProfile(spark,timeWindowStart,"train")
    val videoVector=getVideoVector(spark,timeWindowStart,"train")

    printDf("输入  videoProfile",videoProfile)
    printDf("输入  videoVector",videoVector)

   //3 Process Data
   val df_single_train=getSinglePointTrainSamples(df_orders,userProfile,videoProfile,timeWindowStart,timeWindowEnd)

    //对每个用户计算当前视频与订单历史的相似度和播放历史的相似度两个属性
    val orderHistory=getOrderList(spark,timeWindowStart,"train")
    val playHistory=getPlayList(spark,timeWindowStart,"train")
    val orderSim=getOrderHistorySimilarity(df_single_train.select(col(Dic.colUserId),col(Dic.colVideoId)).dropDuplicates(),orderHistory,videoVector)
    val playSim=getPlayHistorySimilarity(df_single_train.select(col(Dic.colUserId),col(Dic.colVideoId)).dropDuplicates(),playHistory,videoVector)
    //将得到的两个属性添加到样本中
    val df_single_sim=df_single_train.join(orderSim, Seq(Dic.colUserId,Dic.colVideoId),"left")
      .join(playSim, Seq(Dic.colUserId,Dic.colVideoId),"left")

   //选取需要的特征，并对数值型特征进行归一化
   val df_result_scaled=processSample(df_single_sim)
    println("训练数据总条数："+df_result_scaled.count())
    printDf("输出  rankTrainData",df_result_scaled)

    //4 Save  Data
    saveSinglePointTrainSamples(timeWindowStart,timeWindowEnd,df_result_scaled)
    println("RankTrainDatasetGenerate  over~~~~~~~~~~~")




  }

  def processSample(result:DataFrame)={
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
    val df_result=result.select(seqColList.map(result.col(_)):_*)
      .na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)
    val exclude_cols = Array(Dic.colUserId,Dic.colVideoId)
    val df_result_scaled = scaleData(df_result, exclude_cols)
    df_result_scaled


  }
  def getSinglePointTrainSamples(df_orders:DataFrame,userProfile:DataFrame,videoProfile:DataFrame,timeWindowStart:String,timeWindowEnd:String)={
    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)
    //在order订单中选出正样本
    val orderSinglePoint=df_orders
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colCreationTime).>=(timeWindowStart)
          && col(Dic.colCreationTime).<=(timeWindowEnd)
          && col(Dic.colOrderStatus).>(1)
      ).select(col(Dic.colUserId),col(Dic.colResourceId))
      .withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))
      .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    val dataset1=orderSinglePoint.join(userProfile,joinKeysUserId,"inner")
      .join(videoProfile,joinKeysVideoId,"inner")
    println("第一部分数据条数："+dataset1.count())
    //temp.show()

    //第一部分的负样本
    val temp=dataset1.select(col(Dic.colUserId),col(Dic.colVideoId))
    //设置负样本中选择多少个video作为负样本中的video
    val negativeN=20
    val popularVideo=temp.groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))
    val dataset2=temp.select(col(Dic.colUserId)).distinct().crossJoin(popularVideo).except(temp)
      .withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
      .join(userProfile,joinKeysUserId,"inner")
      .join(videoProfile,joinKeysVideoId,"inner")
    println("第二部分数据条数："+dataset2.count())
    // dataset2.show()


    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val negativeSample=10
    val negativeUserN=negativeSample*temp.select(col(Dic.colUserId)).distinct().count()
    val negativeUsers=userProfile.select(col(Dic.colUserId)).except(temp.select(col(Dic.colUserId))).limit(negativeUserN.toInt)
    val dataset3=negativeUsers.crossJoin(popularVideo)
      .withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
      .join(userProfile,joinKeysUserId,"inner")
      .join(videoProfile,joinKeysVideoId,"inner")
      .limit(dataset2.count().toInt)
    println("第三部分数据条数："+dataset3.count())


    dataset1.union(dataset2).union(dataset3)








  }






}
