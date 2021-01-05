package train.userpay

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, getTrainUser, saveProcessedData, saveUserProfilePreferencePart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetLabelAndCount, udfGetLabelAndCount2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProfileGeneratePreferencePartForUserpay {

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)


    val df_train_users = getTrainUser(spark, now)
    printDf("输入 df_train_users", df_train_users)

    // 3 Process Data
    val df_user_profile_pref = userProfileGeneratePreferencePart(now, df_plays, df_train_users, df_medias)

    // 4 Save Data
    saveUserProfilePreferencePart(now, df_user_profile_pref, "train")
    printDf("输出 df_user_profile_pref", df_user_profile_pref)

    println("用户画像Preference部分生成完毕。")

  }

  def userProfileGeneratePreferencePart(now:String, df_plays:DataFrame, df_train_users: DataFrame, df_medias : DataFrame) = {


    val df_train_id = df_train_users.select(Dic.colUserId)
    val df_train_plays = df_plays.join(df_train_id, Seq(Dic.colUserId), "inner")
      .withColumn(Dic.colPlayDate, col(Dic.colPlayStartTime).substr(1, 10))

    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeyVideoId = Seq(Dic.colVideoId)
    val df_train_medias = df_train_plays.join(df_medias, joinKeyVideoId, "inner")

    /**
     * 时长类转换成了分钟
     */
    val df_play_medias_part_41 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast30Days)
      ).withColumn(Dic.colTotalTimeMoviesLast30Days, round(col(Dic.colTotalTimeMoviesLast30Days) / 60, 0))

    val df_play_medias_part_42 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast14Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast14Days, round(col(Dic.colTotalTimeMoviesLast14Days) / 60, 0))


    val df_play_medias_part_43 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast7Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast7Days, round(col(Dic.colTotalTimeMoviesLast7Days) / 60, 0))

    val df_play_medias_part_44 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast3Days)
      ).withColumn(Dic.colTotalTimeMoviesLast3Days, round(col(Dic.colTotalTimeMoviesLast3Days) / 60, 0))


    val df_play_medias_part_45 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast1Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast1Days, round(col(Dic.colTotalTimeMoviesLast1Days) / 60, 0))

    var df_user_profile_pref1 = df_train_id.join(df_play_medias_part_41, joinKeysUserId, "left")
      .join(df_play_medias_part_42, joinKeysUserId, "left")
      .join(df_play_medias_part_43, joinKeysUserId, "left")
      .join(df_play_medias_part_44, joinKeysUserId, "left")
      .join(df_play_medias_part_45, joinKeysUserId, "left")


    val df_play_medias_part_51 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidMoviesLast30Days)
      )
      .withColumn(Dic.colTotalTimePaidMoviesLast30Days, round(col(Dic.colTotalTimePaidMoviesLast30Days) / 60, 0))

    val df_play_medias_part_52 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidMoviesLast14Days)
      )
      .withColumn(Dic.colTotalTimePaidMoviesLast14Days, round(col(Dic.colTotalTimePaidMoviesLast14Days) / 60, 0))


    val df_play_medias_part_53 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidMoviesLast7Days)
      )
      .withColumn(Dic.colTotalTimePaidMoviesLast7Days, round(col(Dic.colTotalTimePaidMoviesLast7Days) / 60, 0))

    val df_play_medias_part_54 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidMoviesLast3Days)
      )
      .withColumn(Dic.colTotalTimePaidMoviesLast3Days, round(col(Dic.colTotalTimePaidMoviesLast3Days) / 60, 0))

    val df_play_medias_part_55 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidMoviesLast1Days)
      )
      .withColumn(Dic.colTotalTimePaidMoviesLast1Days, round(col(Dic.colTotalTimePaidMoviesLast1Days) / 60, 0))


    val df_user_profile_pref2 = df_user_profile_pref1.join(df_play_medias_part_51, joinKeysUserId, "left")
      .join(df_play_medias_part_52, joinKeysUserId, "left")
      .join(df_play_medias_part_53, joinKeysUserId, "left")
      .join(df_play_medias_part_54, joinKeysUserId, "left")
      .join(df_play_medias_part_55, joinKeysUserId, "left")

    /**
     * 休息日 工作日
     */
    val df_play_medias_part_61 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(7)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveWorkdaysLast30Days),
        avg(col(Dic.colTimeSum)).as(Dic.colAvgWorkdailyTimeVideosLast30Days)
      )


    val df_play_medias_part_62 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).===(7)
          && dayofweek(col(Dic.colPlayStartTime)).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveRestdaysLast30Days),
        avg(col(Dic.colTimeSum)).as(Dic.colAvgRestdailyTimeVideosLast30Days)
      )

    val df_play_medias_part_63 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(7)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colTimeSum)).as(Dic.colAvgWorkdailyTimePaidVideosLast30Days)
      )

    val df_play_medias_part_64 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).===(7)
          && dayofweek(col(Dic.colPlayStartTime)).===(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colTimeSum)).as(Dic.colAvgRestdailyTimePaidVideosLast30Days)
      )

    val df_user_profile_pref3 = df_user_profile_pref2.join(df_play_medias_part_61, joinKeysUserId, "left")
      .join(df_play_medias_part_62, joinKeysUserId, "left")
      .join(df_play_medias_part_63, joinKeysUserId, "left")
      .join(df_play_medias_part_64, joinKeysUserId, "left")


    val df_play_medias_part_71_temp = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelPreference),
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colTagPreference)
      )
    val df_play_medias_part_71 = df_play_medias_part_71_temp
      .withColumn(Dic.colVideoOneLevelPreference, udfGetLabelAndCount(col(Dic.colVideoOneLevelPreference)))
      .withColumn(Dic.colVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colVideoTwoLevelPreference)))
      .withColumn(Dic.colTagPreference, udfGetLabelAndCount2(col(Dic.colTagPreference)))


    val df_play_medias_part_72_temp = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colMovieTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colMovieTagPreference)
      )
    val df_play_medias_part_72 = df_play_medias_part_72_temp
      .withColumn(Dic.colMovieTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colMovieTwoLevelPreference)))
      .withColumn(Dic.colMovieTagPreference, udfGetLabelAndCount2(col(Dic.colMovieTagPreference)))


    val df_play_medias_part_73_temp = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colIsSingle).===(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colSingleTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colSingleTagPreference)
      )
    val df_play_medias_part_73 = df_play_medias_part_73_temp
      .withColumn(Dic.colSingleTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colSingleTwoLevelPreference)))
      .withColumn(Dic.colSingleTagPreference, udfGetLabelAndCount2(col(Dic.colSingleTagPreference)))

    val df_play_medias_part_74_temp = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && !isnan(col(Dic.colPackageId))
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colInPackageVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colInPackageTagPreference)
      )
    val df_play_medias_part_74 = df_play_medias_part_74_temp
      .withColumn(Dic.colInPackageVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colInPackageVideoTwoLevelPreference)))
      .withColumn(Dic.colInPackageTagPreference, udfGetLabelAndCount2(col(Dic.colInPackageTagPreference)))


    val df_user_profile_pref = df_user_profile_pref3.join(df_play_medias_part_71, joinKeysUserId, "left")
      .join(df_play_medias_part_72, joinKeysUserId, "left")
      .join(df_play_medias_part_73, joinKeysUserId, "left")
      .join(df_play_medias_part_74, joinKeysUserId, "left")

    df_user_profile_pref

  }


}
