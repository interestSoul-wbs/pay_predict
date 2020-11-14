package predict.common

import mam.Dic
import mam.Utils.{calDate, printDf, udfGetLabelAndCount, udfGetLabelAndCount2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProfileGeneratePreferencePart {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-06-01 00:00:00
    val now = "2020-07-01 00:00:00"

    // 1 - processed df_medias - meida的数据处理相对固定，目前取固定分区 - 2020-10-28，wasu - Konverse - 2020-11-2
//    val df_medias = getMedias(spark)
//
//    printDf("df_medias", df_medias)
//
//    // 2 - processed play data
//    val df_plays = getPlay(spark)
//
//    printDf("df_plays", df_plays)
//
//    // 3 - data process
//    val df_result = userProfileGeneratePreferencePartProcess(now, 30, df_medias, df_plays)
//
//    printDf("df_result", df_result)
//
//    // 4 - save data
//    saveData(spark, df_result)
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


  /**
    * Get user play data.
    *
    * @param spark
    * @return
    */
  def getPlay(spark: SparkSession) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    vodrs.t_sdu_user_play_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }


  /**
    * Save data.
    *
    * @param spark
    * @param df_result
    */
  def saveData(spark: SparkSession, df_result: DataFrame) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.t_sdu_user_profile_preference_paypredict(
        |             user_id string,
        |             total_time_movies_last_30_days double,
        |             total_time_movies_last_14_days double,
        |             total_time_movies_last_7_days double,
        |             total_time_movies_last_3_days double,
        |             total_time_movies_last_1_days double,
        |             total_time_paid_movies_last_30_days double,
        |             total_time_paid_movies_last_14_days double,
        |             total_time_paid_movies_last_7_days double,
        |             total_time_paid_movies_last_3_days double,
        |             total_time_paid_movies_last_1_days double,
        |             active_workdays_last_30_days long,
        |             avg_workdaily_time_videos_last_30_days double,
        |             active_restdays_last_30_days long,
        |             avg_restdaily_time_videos_last_30_days double,
        |             avg_workdaily_time_paid_videos_last_30_days double,
        |             avg_restdaily_time_paid_videos_last_30_days double,
        |             video_one_level_preference map<string, int>,
        |             video_two_level_preference map<string, int>,
        |             tag_preference map<string, int>,
        |             movie_two_level_preference map<string, int>,
        |             movie_tag_preference map<string, int>,
        |             single_two_level_preference map<string, int>,
        |             single_tag_preference map<string, int>,
        |             in_package_video_two_level_preference map<string, int>,
        |             in_package_tag_preference map<string, int>
        |             )
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_sdu_user_profile_preference_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |     user_id,
         |     total_time_movies_last_30_days,
         |     total_time_movies_last_14_days,
         |     total_time_movies_last_7_days,
         |     total_time_movies_last_3_days,
         |     total_time_movies_last_1_days,
         |     total_time_paid_movies_last_30_days,
         |     total_time_paid_movies_last_14_days,
         |     total_time_paid_movies_last_7_days,
         |     total_time_paid_movies_last_3_days,
         |     total_time_paid_movies_last_1_days,
         |     active_workdays_last_30_days,
         |     avg_workdaily_time_videos_last_30_days,
         |     active_restdays_last_30_days,
         |     avg_restdaily_time_videos_last_30_days,
         |     avg_workdaily_time_paid_videos_last_30_days,
         |     avg_restdaily_time_paid_videos_last_30_days,
         |     video_one_level_preference,
         |     video_two_level_preference,
         |     tag_preference,
         |     movie_two_level_preference,
         |     movie_tag_preference,
         |     single_two_level_preference,
         |     single_tag_preference,
         |     in_package_video_two_level_preference,
         |     in_package_tag_preference
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
