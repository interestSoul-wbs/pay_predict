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

    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true") //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()

    val df_orders = getProcessedOrder(partitiondate, license)

    val df_user_profile_play_part = getUserProfilePlayPart(partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(partitiondate, license, "valid")

    val df_video_profile = getVideoProfile(partitiondate, license, "valid")

    val df_video_vector = getVideoVector(partitiondate, license)

    val df_user_division_result = getUserDivisionResult(partitiondate, license, "predict")

    val joinKeysUserId = Seq(Dic.colUserId)
    val joinKeysVideoId = Seq(Dic.colVideoId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    //由于内存的限制，设置预测的单点视频的数量，如果内存足够大可以将单点视频的数量设置为媒资中所有单点视频的数量
    val selectN = 10
    val df_select_singlepoint = df_orders.filter(
      col(Dic.colResourceType).===(0)
        && col(Dic.colCreationTime).<(args(0))
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(count(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(selectN)
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId))

    val df_select_videos = df_select_singlepoint
      .join(df_video_profile, joinKeysVideoId, "inner")

    val df_select_users = df_user_division_result
      .select(
        col(Dic.colUserId))
      .join(df_user_profile, joinKeysUserId, "inner")

//    println("预测的用户的数量：" + df_select_users.count())

    val df_result_tmp_1 = df_select_users
      .crossJoin(df_select_videos)

//    println("预测的数据的条数：" + df_result_tmp_1.count())

    val seqColList = getNumerableColsSeq(df_result_tmp_1)

    val df_result_tmp_2 = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)

    // MinMaxScaler
    val exclude_cols = Array(Dic.colUserId, Dic.colVideoId)

    val df_scaled_data = scaleData(df_result_tmp_2, exclude_cols)

    // scaled data join w2v vectors
    val df_result = df_scaled_data
      .join(df_video_vector, joinKeysVideoId, "left")
      .na.fill(0)
      .withColumn(Dic.colOrderStatus, lit(-1))

    println("总样本的条数" + df_result.count())

    println(df_result.printSchema())

    //    printDf("df_result", df_result)

    saveSinglepointRankData(df_result, partitiondate, license, "predict")
  }
}
