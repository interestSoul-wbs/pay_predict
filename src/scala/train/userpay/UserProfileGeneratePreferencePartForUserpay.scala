package train.userpay

import mam.Dic
import mam.Utils.{calDate, getData, saveProcessedData, udfGetLabelAndCount, udfGetLabelAndCount2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProfileGeneratePreferencePartForUserpay {

  def userProfileGeneratePreferencePart(now: String, df_medias: DataFrame, df_plays: DataFrame, df_trainUsers: DataFrame, userProfilePreferencePartSavePath: String) = {


    val df_trainId = df_trainUsers.select(Dic.colUserId)
    val df_trainUserPlays = df_plays.join(df_trainId, Seq(Dic.colUserId), "inner")

    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeyVideoId = Seq(Dic.colVideoId)
    val df_trainUsersMedias = df_trainUserPlays.join(df_medias, joinKeyVideoId, "inner")

    /**
     * 时长类转换成了分钟
     */
    val play_medias_part_41 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast30Days)
      ).withColumn(Dic.colTotalTimeMoviesLast30Days, round(col(Dic.colTotalTimeMoviesLast30Days) / 60, 0))

    val play_medias_part_42 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast14Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast14Days, round(col(Dic.colTotalTimeMoviesLast14Days) / 60, 0))


    val play_medias_part_43 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast7Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast7Days, round(col(Dic.colTotalTimeMoviesLast7Days) / 60, 0))

    val play_medias_part_44 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast3Days)
      ).withColumn(Dic.colTotalTimeMoviesLast3Days, round(col(Dic.colTotalTimeMoviesLast3Days) / 60, 0))


    val play_medias_part_45 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeMoviesLast1Days)
      )
      .withColumn(Dic.colTotalTimeMoviesLast1Days, round(col(Dic.colTotalTimeMoviesLast1Days) / 60, 0))

    var df_trainUserProfilePref = df_trainId.join(play_medias_part_41, joinKeysUserId, "left")
      .join(play_medias_part_42, joinKeysUserId, "left")
      .join(play_medias_part_43, joinKeysUserId, "left")
      .join(play_medias_part_44, joinKeysUserId, "left")
      .join(play_medias_part_45, joinKeysUserId, "left")


    val play_medias_part_51 = df_trainUsersMedias
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

    val play_medias_part_52 = df_trainUsersMedias
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


    val play_medias_part_53 = df_trainUsersMedias
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

    val play_medias_part_54 = df_trainUsersMedias
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

    val play_medias_part_55 = df_trainUsersMedias
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


    df_trainUserProfilePref = df_trainUserProfilePref.join(play_medias_part_51, joinKeysUserId, "left")
      .join(play_medias_part_52, joinKeysUserId, "left")
      .join(play_medias_part_53, joinKeysUserId, "left")
      .join(play_medias_part_54, joinKeysUserId, "left")
      .join(play_medias_part_55, joinKeysUserId, "left")


    val play_medias_part_61 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(7)
          && dayofweek(col(Dic.colPlayStartTime)).=!=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayStartTime)).as(Dic.colActiveWorkdaysLast30Days),
        avg(col(Dic.colTimeSum)).as(Dic.colAvgWorkdailyTimeVideosLast30Days)
      )


    val play_medias_part_62 = df_trainUsersMedias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayStartTime)).===(7)
          && dayofweek(col(Dic.colPlayStartTime)).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayStartTime)).as(Dic.colActiveRestdaysLast30Days),
        avg(col(Dic.colTimeSum)).as(Dic.colAvgRestdailyTimeVideosLast30Days)
      )

    val play_medias_part_63 = df_trainUsersMedias
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

    val play_medias_part_64 = df_trainUsersMedias
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

    df_trainUserProfilePref = df_trainUserProfilePref.join(play_medias_part_61, joinKeysUserId, "left")
      .join(play_medias_part_62, joinKeysUserId, "left")
      .join(play_medias_part_63, joinKeysUserId, "left")
      .join(play_medias_part_64, joinKeysUserId, "left")


    val play_medias_part_71_temp = df_trainUsersMedias
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
    val play_medias_part_71 = play_medias_part_71_temp
      .withColumn(Dic.colVideoOneLevelPreference, udfGetLabelAndCount(col(Dic.colVideoOneLevelPreference)))
      .withColumn(Dic.colVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colVideoTwoLevelPreference)))
      .withColumn(Dic.colTagPreference, udfGetLabelAndCount2(col(Dic.colTagPreference)))


    val play_medias_part_72_temp = df_trainUsersMedias
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
    val play_medias_part_72 = play_medias_part_72_temp
      .withColumn(Dic.colMovieTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colMovieTwoLevelPreference)))
      .withColumn(Dic.colMovieTagPreference, udfGetLabelAndCount2(col(Dic.colMovieTagPreference)))


    val play_medias_part_73_temp = df_trainUsersMedias
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
    val play_medias_part_73 = play_medias_part_73_temp
      .withColumn(Dic.colSingleTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colSingleTwoLevelPreference)))
      .withColumn(Dic.colSingleTagPreference, udfGetLabelAndCount2(col(Dic.colSingleTagPreference)))

    val play_medias_part_74_temp = df_trainUsersMedias
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
    val play_medias_part_74 = play_medias_part_74_temp
      .withColumn(Dic.colInPackageVideoTwoLevelPreference, udfGetLabelAndCount2(col(Dic.colInPackageVideoTwoLevelPreference)))
      .withColumn(Dic.colInPackageTagPreference, udfGetLabelAndCount2(col(Dic.colInPackageTagPreference)))


    df_trainUserProfilePref = df_trainUserProfilePref.join(play_medias_part_71, joinKeysUserId, "left")
      .join(play_medias_part_72, joinKeysUserId, "left")
      .join(play_medias_part_73, joinKeysUserId, "left")
      .join(play_medias_part_74, joinKeysUserId, "left")


    //大约有85万用户
    saveProcessedData(df_trainUserProfilePref, userProfilePreferencePartSavePath)
    println("df_trainUserProfilePref Save Done!")

  }


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGeneratePreferencePartForUserpayTrain")
      .master("local[6]")
      .getOrCreate()


    val now = args(0) + " " + args(1)

    //val hdfsPath = "hdfs:///pay_predict/"
    val hdfsPath=""
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3" //userpay
    val trainUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)
    val userProfilePreferencePartSavePath = hdfsPath + "data/train/common/processed/userpay/userprofilepreferencepart" + args(0)

    /**
     * Get Data
     */
    val df_medias = getData(spark, mediasProcessedPath)
    val df_plays = getData(spark, playsProcessedPath)
    val df_trainUsers = getData(spark, trainUsersPath)


    userProfileGeneratePreferencePart(now, df_medias, df_plays, df_trainUsers, userProfilePreferencePartSavePath)


  }

}
