package train.singlepoint

import mam.Dic
import mam.Utils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import mam.GetSaveData._
import com.github.nscala_time.time.Imports._
import scala.collection.mutable.ArrayBuffer

object RankTrainDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10
  var date: DateTime = _
  var thirtyDaysAgo: String = _
  var sixteenDaysAgo: String = _
  var vectorDimension: Int = 64

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    thirtyDaysAgo = (date - 30.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))
    sixteenDaysAgo = (date - 16.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val predictWindowStart = thirtyDaysAgo // 例： 2020-09-01 00:00:00

    println("predictWindowStart is : " + thirtyDaysAgo)

    val predictWindowEnd = sixteenDaysAgo // 例： 2020-09-15 00:00:00

    println("predictWindowEnd is : " + sixteenDaysAgo)

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
        udfConvertLabel(col(Dic.colOrderStatus)).as(Dic.colOrderStatus))

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
    val df_all_profile_tmp_3 = df_neg_users
      .crossJoin(df_popular_video)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第三部分数据条数：" + df_all_profile_tmp_3.count())

    printDf("df_all_profile_tmp_3", df_all_profile_tmp_3)

    val df_result_tmp_1 = df_all_profile_tmp_1
      .union(df_all_profile_tmp_2)
      .union(df_all_profile_tmp_3)

    printDf("df_result_tmp_1", df_result_tmp_1)

    val seqColList = getNumerableColsSeq(df_result_tmp_1)

    val df_result_tmp_2 = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)

    printDf("df_result_tmp_2", df_result_tmp_2)

    // MinMaxScaler
    val exclude_cols = Array(Dic.colUserId, Dic.colVideoId, Dic.colOrderStatus)

    val df_scaled_data = scaleData(df_result_tmp_2, exclude_cols)

    printDf("df_scaled_data", df_scaled_data)

    // scaled data join w2v vectors
    val df_result = df_scaled_data
      .join(df_video_vector, joinKeysVideoId, "left")
      .na.fill(0)

    println("总样本的条数" + df_result.count())

//    printDf("df_result", df_result)

    saveSinglepointRankData(spark, df_result, partitiondate, license, "train")
  }

  def getW2vColsArray(w2v_demension: Int) = {

    val w2vColList = ArrayBuffer[String]()

    for (i <- 0 to w2v_demension) {
      w2vColList.append("v_" + i.toString)
      println(i)
    }

    w2vColList.toArray
  }

  def udfConvertLabel = udf(convertLabel _)

  def convertLabel(input_value: Double) = {

    var result = 0.0

    if (input_value > 1) {
      result = 1.0
    } else {
      result = 0.0
    }

    result
  }
}
