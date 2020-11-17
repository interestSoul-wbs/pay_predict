package train.singlepoint

import mam.Dic
import mam.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import mam.GetSaveData._

object RankTrainDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true") //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "train")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "train")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "train")

    val df_video_profile = getVideoProfile(spark, partitiondate, license, "train")

    val df_video_vector = getVideoVector(spark, partitiondate, license)

    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeysVideoId = Seq(Dic.colVideoId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)

    val predictWindowStart = "2020-09-01 00:00:00"

    val predictWindowEnd = "2020-09-07 00:00:00"

    //在order订单中选出正样本
    val df_order_single_point = df_orders
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colCreationTime).>=(predictWindowStart)
          && col(Dic.colCreationTime).<=(predictWindowEnd)
          && col(Dic.colOrderStatus).>(1))
      .select(col(Dic.colUserId), col(Dic.colResourceId), col(Dic.colOrderStatus))
      .withColumnRenamed(Dic.colResourceId, Dic.colVideoId)

    val df_all_profile_tmp_1 = df_order_single_point
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    //第一部分的负样本
    val df_userid_videoid = df_all_profile_tmp_1.select(col(Dic.colUserId), col(Dic.colVideoId))

    //设置负样本中选择多少个video作为负样本中的video
    val df_popular_video = df_userid_videoid
      .groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))

    val df_all_profile_tmp_2 = df_userid_videoid
      .select(col(Dic.colUserId))
      .distinct()
      .crossJoin(df_popular_video)
      .except(df_userid_videoid)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第二部分数据条数：" + df_all_profile_tmp_2.count())

    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val df_result_tmp_1 = df_all_profile_tmp_1
      .union(df_all_profile_tmp_2)
      .join(df_video_vector, joinKeysVideoId, "left")

    val colTypeList = df_result_tmp_1.dtypes.toList

    val colList = ArrayBuffer[String]()

    colList.append(Dic.colUserId)
    colList.append(Dic.colVideoId)

    for (elem <- colTypeList) {
      if (elem._2.equals("IntegerType") || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        colList.append(elem._1)
      }
    }
    
    colList -= Dic.colIsSingle
    colList -= Dic.colIsTrailers
    colList -= Dic.colIsPaid

    val seqColList = colList.toList

    val df_result = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)

    println("总样本的条数" + df_result.count())

    printDf("df_result", df_result)
  }

  def saveRankTrainData() = {

  }

}
