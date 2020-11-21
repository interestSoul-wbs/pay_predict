package predict.common.ori.singlepointbackup

import mam.Dic
import mam.GetSaveData._
import mam.Utils.{getNumerableColsSeq, printDf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}

object RankPredictDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true") //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "valid")

    val df_video_profile = getVideoProfile(spark, partitiondate, license, "valid")

    val df_video_vector = getVideoVector(spark, partitiondate, license)

    val df_user_division = getUserDivisionResult(spark, partitiondate, license, "valid")

    printDf("df_user_division", df_user_division)

    val joinKeysUserId = Seq(Dic.colUserId)
    val joinKeysVideoId = Seq(Dic.colVideoId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    //由于内存的限制，设置预测的单点视频的数量，如果内存足够大可以将单点视频的数量设置为媒资中所有单点视频的数量
    val selectN = 20
    val df_select_singlepoint = df_orders.filter(
      col(Dic.colResourceType).===(0)
        && col(Dic.colCreationTime).<(args(0))
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(count(col(Dic.colUserId)).as(Dic.colCount))
      .orderBy(col(Dic.colCount).desc)
      .limit(selectN)
      .select(col(Dic.colResourceId))
      .withColumnRenamed(Dic.colResourceId, Dic.colVideoId)

    val df_select_videos = df_select_singlepoint.join(df_video_profile, joinKeysVideoId, "inner")

    val df_select_users = df_user_division
      .select(
        col(Dic.colUserId))
      .join(df_user_profile, joinKeysUserId, "inner")

    println("预测的用户的数量：" + df_select_users.count())

    val df_result_tmp_1 = df_select_users
      .crossJoin(df_select_videos)
      .join(df_video_vector, joinKeysVideoId, "left")

    printDf("df_result_tmp_1", df_result_tmp_1)

    val seqColList = getNumerableColsSeq(df_result_tmp_1)

    val df_result = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)
      .na.drop()

    println("总样本的条数" + df_result.count())

    printDf("df_result", df_result)

    saveSinglepointRankData(spark, df_result, partitiondate, license, "valid")
  }

}
