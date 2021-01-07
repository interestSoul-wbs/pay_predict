package common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, saveUserProfilePlayData}
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserProfileGeneratePlayPart {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var date: DateTime = _
  var nDaysFromStartDate: Int = _
  var dataSplitDate: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // union1.x
    sector = args(3).toInt
    nDaysFromStartDate = args(4).toInt // 1/7/14 - 各跑一次

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    dataSplitDate = (date - (30 - nDaysFromStartDate).days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // 训练集的划分时间点 - 输入时间的30天之前
    println("data date is : " + dataSplitDate)

    // 1 - processed medias -
    val df_medias = getProcessedMedias(partitiondate, license)

    printDf("df_medias", df_medias)

    // 2 - processed play data
    val df_plays = getProcessedPlay(partitiondate, license, vodVersion, sector)

    printDf("df_plays", df_plays)

    // 3 - data process
    val df_result = userProfileGeneratePlayPart(dataSplitDate, 30, df_medias, df_plays)

    printDf("df_result", df_result)

    // 4 - save data
    saveUserProfilePlayData(df_result, partitiondate, license, vodVersion, sector, nDaysFromStartDate)
  }

  def userProfileGeneratePlayPart(now: String, timeWindow: Int, df_medias: DataFrame, df_plays: DataFrame) = {

    val df_user_id = df_plays
      .select(col(Dic.colUserId)).distinct()

    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)

    val df_play_part_1 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveDaysLast30Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast30Days),
        udfGetDays(max(col(Dic.colPlayEndTime)), lit(now)).as(Dic.colDaysFromLastActive),
        udfGetDays(min(col(Dic.colPlayEndTime)), lit(now)).as(Dic.colDaysSinceFirstActiveInTimewindow))

    val df_play_part_2 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveDaysLast14Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast14Days))

    val df_play_part_3 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveDaysLast7Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast7Days))

    val df_play_part_4 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveDaysLast3Days),
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeLast3Days))

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_result_tmp_1 = df_user_id
      .join(df_play_part_1, joinKeysUserId, "left")
      .join(df_play_part_2, joinKeysUserId, "left")
      .join(df_play_part_3, joinKeysUserId, "left")
      .join(df_play_part_4, joinKeysUserId, "left")

    val joinKeyVideoId = Seq(Dic.colVideoId)

    val df_user_medias = df_plays.join(df_medias, joinKeyVideoId, "inner")

    val df_play_medias_part_11 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast30Days))

    val df_play_medias_part_12 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast14Days))

    val df_play_medias_part_13 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast7Days))

    val df_play_medias_part_14 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast3Days))

    val df_play_medias_part_15 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidVideosLast1Days))

    val df_result_tmp_2 = df_result_tmp_1
      .join(df_play_medias_part_11, joinKeysUserId, "left")
      .join(df_play_medias_part_12, joinKeysUserId, "left")
      .join(df_play_medias_part_13, joinKeysUserId, "left")
      .join(df_play_medias_part_14, joinKeysUserId, "left")
      .join(df_play_medias_part_15, joinKeysUserId, "left")

    df_result_tmp_1.unpersist()

    val df_play_medias_part_21 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast30Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast30Days))

    val df_play_medias_part_22 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast14Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast14Days))

    val df_play_medias_part_23 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast7Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast7Days))

    val df_play_medias_part_24 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast3Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast3Days))

    val df_play_medias_part_25 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeInPackageVideosLast1Days),
        stddev(col(Dic.colBroadcastTime)).as(Dic.colVarTimeInPackageVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast1Days))

    val df_result_tmp_3 = df_result_tmp_2
      .join(df_play_medias_part_21, joinKeysUserId, "left")
      .join(df_play_medias_part_22, joinKeysUserId, "left")
      .join(df_play_medias_part_23, joinKeysUserId, "left")
      .join(df_play_medias_part_24, joinKeysUserId, "left")
      .join(df_play_medias_part_25, joinKeysUserId, "left")

    df_result_tmp_2.unpersist()

    val df_play_medias_part_31 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast30Days))

    val df_play_medias_part_32 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast14Days))

    val df_play_medias_part_33 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast7Days))

    val df_play_medias_part_34 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast3Days))

    val df_play_medias_part_35 = df_user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeChildrenVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast1Days))

    val df_result = df_result_tmp_3
      .join(df_play_medias_part_31, joinKeysUserId, "left")
      .join(df_play_medias_part_32, joinKeysUserId, "left")
      .join(df_play_medias_part_33, joinKeysUserId, "left")
      .join(df_play_medias_part_34, joinKeysUserId, "left")
      .join(df_play_medias_part_35, joinKeysUserId, "left")

    df_result_tmp_3.unpersist()

    df_result
  }
}
