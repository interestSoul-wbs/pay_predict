package predict.singlepoint

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RankPredictDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10
  var date: DateTime = _
  var fifteenDaysAgo: String = _
  var oneDayAgo: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    fifteenDaysAgo = (date - 15.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))
    oneDayAgo = (date - 1.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val predictWindowStart = fifteenDaysAgo // 例： 2020-09-16 00:00:00

    println("predictWindowStart is : " + fifteenDaysAgo)

    val predictWindowEnd = oneDayAgo // 例： 2020-09-30 00:00:00

    println("predictWindowEnd is : " + oneDayAgo)

    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true") //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "valid")

    val df_video_profile = getVideoProfile(spark, partitiondate, license, "valid")

    val df_video_vector = getVideoVector(spark, partitiondate, license)

    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeysVideoId = Seq(Dic.colVideoId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)
    
    //这一部分是用户划分模型通过预测得到的结果，主要是一份用户清单，表示会购买单点视频的用户，需要改写一下
    val userDivisionResultPath=hdfsPath+"data/predict/singlepoint/userdivisionresult"+args(0)+"-"+args(2)
    val userDivisionResult=spark.read.format("parquet").load(userDivisionResultPath)
    
    
    //由于内存的限制，设置预测的单点视频的数量，如果内存足够大可以将单点视频的数量设置为媒资中所有单点视频的数量
    val selectN=20
    var selectSinglePoint=orders.filter(
      col(Dic.colResourceType).===(0)
      && col(Dic.colCreationTime).<(args(0))
      && col(Dic.colOrderStatus).>(1)
    ).groupBy(col(Dic.colResourceId))
      .agg(count(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(selectN)
      .select(col(Dic.colResourceId))
      .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    var selectVideos=selectSinglePoint.join(videoProfile,joinKeysVideoId,"inner")


    var selectUsers=userDivisionResult.select(col(Dic.colUserId)).join(userProfile,joinKeysUserId,"inner")
    println("预测的用户的数量："+selectUsers.count())
    var result=selectUsers.crossJoin(selectVideos)
    result=result.join(videoVector,joinKeysVideoId,"left")
    println("预测的数据的条数："+result.count())



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
      .na.drop()
    //result.show()
    println("总样本的条数"+result.count())

    
    
    
    
     /*
    //在order订单中选出正样本
    val df_order_single_point = df_orders
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colCreationTime).>=(predictWindowStart)
          && col(Dic.colCreationTime).<=(predictWindowEnd)
          && col(Dic.colOrderStatus).>(1))
      .select(
        col(Dic.colUserId),
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colOrderStatus))

    printDf("df_order_single_point", df_order_single_point)

    val df_all_profile_tmp_1 = df_order_single_point
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    printDf("df_all_profile_tmp_1", df_all_profile_tmp_1)

    println("第一部分数据条数：" + df_all_profile_tmp_1.count())

    //第一部分的负样本
    val df_userid_videoid = df_all_profile_tmp_1.select(col(Dic.colUserId), col(Dic.colVideoId))

    printDf("df_userid_videoid", df_userid_videoid)

    //设置负样本中选择多少个video作为负样本中的video
    val df_popular_video = df_userid_videoid
      .groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))

    printDf("df_popular_video", df_popular_video)

    val df_all_profile_tmp_2 = df_userid_videoid
      .select(col(Dic.colUserId))
      .distinct()
      .crossJoin(df_popular_video)
      .except(df_userid_videoid)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第二部分数据条数：" + df_all_profile_tmp_2.count())

    printDf("df_all_profile_tmp_2", df_all_profile_tmp_2)

    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val negativeUserN = 10 * df_userid_videoid.select(col(Dic.colUserId)).distinct().count()
    val df_neg_users = df_user_profile.select(col(Dic.colUserId)).except(df_userid_videoid.select(col(Dic.colUserId))).limit(negativeUserN.toInt)
    var df_all_profile_tmp_3 = df_neg_users
      .crossJoin(df_popular_video)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第三部分数据条数：" + df_all_profile_tmp_3.count())

    printDf("df_all_profile_tmp_3", df_all_profile_tmp_3)

    val df_result_tmp_1 = df_all_profile_tmp_1
      .union(df_all_profile_tmp_2)
      .union(df_all_profile_tmp_3)
      .join(df_video_vector, joinKeysVideoId, "left")

    printDf("df_result_tmp_1", df_result_tmp_1)

    val seqColList = getNumerableColsSeq(df_result_tmp_1)

    val df_result = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)

    println("总样本的条数" + df_result.count())
    */

    printDf("df_result", df_result)

    saveSinglepointRankData(spark, df_result, partitiondate, license, "valid")
  }
}
