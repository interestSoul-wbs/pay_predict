package common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, saveUserProfileGeneratePreferencePart}
import mam.Utils.{calDate, printDf, udfGetLabelAndCount, udfGetLabelAndCount2}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserProfileGeneratePreferencePart {

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
    vodVersion = args(2) // 2020-12-1 - union1.x
    sector = args(3).toInt
    nDaysFromStartDate = args(4).toInt // 1/7/14 - 各跑一次

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    dataSplitDate = (date - (30 - nDaysFromStartDate).days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 1 - processed media
    val df_medias = getProcessedMedias(partitiondate, license)

    printDf("df_medias", df_medias)

    // 2 - processed play
    val df_plays = getProcessedPlay(partitiondate, license, vodVersion, sector)

    printDf("df_plays", df_plays)

    // 3 - data process
    val df_result = userProfileGeneratePreferencePartProcess(dataSplitDate, 30, df_medias, df_plays)

    printDf("df_result", df_result)

    // 4 - save data
    saveUserProfileGeneratePreferencePart(df_result, partitiondate, license, vodVersion, sector, nDaysFromStartDate)
  }

  def userProfileGeneratePreferencePartProcess(now: String, timeWindow: Int, df_medias: DataFrame, df_plays: DataFrame) = {

    val df_user_id = df_plays
      .select(col(Dic.colUserId)).distinct()

    val pre_30 = calDate(now, -30)

    val pre_14 = calDate(now, days = -14)

    val pre_7 = calDate(now, -7)

    val pre_3 = calDate(now, -3)

    val pre_1 = calDate(now, -1)

    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeyVideoId = Seq(Dic.colVideoId)

    val user_medias = df_plays.join(df_medias, joinKeyVideoId, "inner")

    val play_medias_part_41 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast30Days))

    val play_medias_part_42 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast14Days))

    val play_medias_part_43 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast7Days))

    val play_medias_part_44 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast3Days))

    val play_medias_part_45 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast1Days))

    val df_result_tmp_1 = df_user_id
      .join(play_medias_part_41, joinKeysUserId, "left")
      .join(play_medias_part_42, joinKeysUserId, "left")
      .join(play_medias_part_43, joinKeysUserId, "left")
      .join(play_medias_part_44, joinKeysUserId, "left")
      .join(play_medias_part_45, joinKeysUserId, "left")

    val play_medias_part_51 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast30Days))

    val play_medias_part_52 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast14Days))

    val play_medias_part_53 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast7Days))

    val play_medias_part_54 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast3Days))

    val play_medias_part_55 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast1Days))

    val df_result_tmp_2 = df_result_tmp_1
      .join(play_medias_part_51, joinKeysUserId, "left")
      .join(play_medias_part_52, joinKeysUserId, "left")
      .join(play_medias_part_53, joinKeysUserId, "left")
      .join(play_medias_part_54, joinKeysUserId, "left")
      .join(play_medias_part_55, joinKeysUserId, "left")

    df_result_tmp_1.unpersist()

    val play_medias_part_61 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(7)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveWorkdaysLast30Days),
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgWorkdailyTimeVideosLast30Days))

    val play_medias_part_62 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).===(7)
          && dayofweek(col(Dic.colPlayEndTime)).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveRestdaysLast30Days),
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgRestdailyTimeVideosLast30Days))

    val play_medias_part_63 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(7)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgWorkdailyTimePaidVideosLast30Days))

    val play_medias_part_64 = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).===(7)
          && dayofweek(col(Dic.colPlayEndTime)).===(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgRestdailyTimePaidVideosLast30Days))

    val df_result_tmp_3 = df_result_tmp_2.join(play_medias_part_61, joinKeysUserId, "left")
      .join(play_medias_part_62, joinKeysUserId, "left")
      .join(play_medias_part_63, joinKeysUserId, "left")
      .join(play_medias_part_64, joinKeysUserId, "left")

    df_result_tmp_2.unpersist()

    val play_medias_part_71_temp = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30))
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelPreference),
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colTagPreference))

    val play_medias_part_71 = play_medias_part_71_temp
      .withColumn(Dic.colVideoOneLevelPreference, udfGetLabelAndCount(col(Dic.colVideoOneLevelPreference)))
      .withColumn(Dic.colVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colVideoTwoLevelPreference)))
      .withColumn(Dic.colTagPreference, udfGetLabelAndCount2(col(Dic.colTagPreference)))

    val play_medias_part_72_temp = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colMovieTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colMovieTagPreference))

    val play_medias_part_72 = play_medias_part_72_temp
      .withColumn(Dic.colMovieTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colMovieTwoLevelPreference)))
      .withColumn(Dic.colMovieTagPreference, udfGetLabelAndCount2(col(Dic.colMovieTagPreference)))

    val play_medias_part_73_temp = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colIsSingle).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colSingleTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colSingleTagPreference))

    val play_medias_part_73 = play_medias_part_73_temp
      .withColumn(Dic.colSingleTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colSingleTwoLevelPreference)))
      .withColumn(Dic.colSingleTagPreference, udfGetLabelAndCount2(col(Dic.colSingleTagPreference)))

    val play_medias_part_74_temp = user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colInPackageVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colInPackageTagPreference))

    val play_medias_part_74 = play_medias_part_74_temp
      .withColumn(Dic.colInPackageVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colInPackageVideoTwoLevelPreference)))
      .withColumn(Dic.colInPackageTagPreference, udfGetLabelAndCount2(col(Dic.colInPackageTagPreference)))

    val df_result = df_result_tmp_3
      .join(play_medias_part_71, joinKeysUserId, "left")
      .join(play_medias_part_72, joinKeysUserId, "left")
      .join(play_medias_part_73, joinKeysUserId, "left")
      .join(play_medias_part_74, joinKeysUserId, "left")

    df_result_tmp_3.unpersist()

    df_result
  }
}
