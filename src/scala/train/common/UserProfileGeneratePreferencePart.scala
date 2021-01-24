package train.common

import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, saveUserProfilePreferencePart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfGetLabelAndCount, udfGetLabelAndCount2}
import mam.{Dic, SparkSessionInit}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProfileGeneratePreferencePart {
  def main(args:Array[String]): Unit ={
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)


    // 3 Process Data
    val df_user_profile_pref = userProfileGeneratePreferencePart(now, df_plays, df_medias)

    // 4 Save Data
    saveUserProfilePreferencePart(now, df_user_profile_pref, "train")
    printDf("输出 df_user_profile_pref", df_user_profile_pref)

    println("UserProfileGeneratePreferencePart  over~~~~~~~~~~~")



  }

  def userProfileGeneratePreferencePart(now:String,df_plays:DataFrame,df_medias:DataFrame)= {

    val  df_train_id=df_plays.select(col(Dic.colUserId)).distinct()
    val df_train_plays=df_plays



    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeyVideoId=Seq(Dic.colVideoId)
    val df_train_medias=df_train_plays.join(df_medias,joinKeyVideoId,"inner")


    val play_medias_part_41=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast30Days)
      )
    val play_medias_part_42=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast14Days)
      )
    val play_medias_part_43=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast7Days)
      )
    val play_medias_part_44=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast3Days)
      )
    val play_medias_part_45=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast1Days)
      )

    var df_user_profile_pref1 = df_train_id.join(play_medias_part_41,joinKeysUserId,"left")
      .join(play_medias_part_42,joinKeysUserId, "left")
      .join(play_medias_part_43,joinKeysUserId,"left")
      .join(play_medias_part_44,joinKeysUserId, "left")
      .join(play_medias_part_45,joinKeysUserId, "left")



    val play_medias_part_51=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast30Days)
      )
    val play_medias_part_52=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast14Days)
      )
    val play_medias_part_53=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast7Days)
      )
    val play_medias_part_54=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast3Days)
      )
    val play_medias_part_55=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast1Days)
      )
    val df_user_profile_pref2 = df_user_profile_pref1.join(play_medias_part_51,joinKeysUserId,"left")
      .join(play_medias_part_52,joinKeysUserId, "left")
      .join(play_medias_part_53,joinKeysUserId,"left")
      .join(play_medias_part_54,joinKeysUserId, "left")
      .join(play_medias_part_55,joinKeysUserId, "left")



    val play_medias_part_61=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(7)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveWorkdaysLast30Days),
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgWorkdailyTimeVideosLast30Days)
      )
    val play_medias_part_62=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).===(7)
          && dayofweek(col(Dic.colPlayEndTime)).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayEndTime)).as(Dic.colActiveRestdaysLast30Days),
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgRestdailyTimeVideosLast30Days)
      )
    val play_medias_part_63=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(7)
          && dayofweek(col(Dic.colPlayEndTime)).=!=(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgWorkdailyTimePaidVideosLast30Days)
      )
    val play_medias_part_64=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && dayofweek(col(Dic.colPlayEndTime)).===(7)
          && dayofweek(col(Dic.colPlayEndTime)).===(1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        avg(col(Dic.colBroadcastTime)).as(Dic.colAvgRestdailyTimePaidVideosLast30Days)
      )
    val df_user_profile_pref3 = df_user_profile_pref2.join(play_medias_part_61,joinKeysUserId,"left")
      .join(play_medias_part_62,joinKeysUserId, "left")
      .join(play_medias_part_63,joinKeysUserId,"left")
      .join(play_medias_part_64,joinKeysUserId, "left")





    val play_medias_part_71_temp=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelPreference),
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colTagPreference)
      )
    val play_medias_part_71=play_medias_part_71_temp
      .withColumn(Dic.colVideoOneLevelPreference,udfGetLabelAndCount(col(Dic.colVideoOneLevelPreference)))
      .withColumn(Dic.colVideoTwoLevelPreference,udfGetLabelAndCount2(col(Dic.colVideoTwoLevelPreference)))
      .withColumn(Dic.colTagPreference,udfGetLabelAndCount2(col(Dic.colTagPreference)))


    val play_medias_part_72_temp=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colMovieTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colMovieTagPreference)
      )
    val play_medias_part_72=play_medias_part_72_temp
      .withColumn(Dic.colMovieTwoLevelPreference,udfGetLabelAndCount2(col(Dic.colMovieTwoLevelPreference)))
      .withColumn(Dic.colMovieTagPreference,udfGetLabelAndCount2(col(Dic.colMovieTagPreference)))



    val play_medias_part_73_temp=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colIsSingle).===(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colSingleTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colSingleTagPreference)
      )
    val play_medias_part_73=play_medias_part_73_temp
      .withColumn(Dic.colSingleTwoLevelPreference,udfGetLabelAndCount2(col(Dic.colSingleTwoLevelPreference)))
      .withColumn(Dic.colSingleTagPreference,udfGetLabelAndCount2(col(Dic.colSingleTagPreference)))

    val play_medias_part_74_temp=df_train_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && !isnan(col(Dic.colPackageId))
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col(Dic.colVideoTwoLevelClassificationList)).as(Dic.colInPackageVideoTwoLevelPreference),
        collect_list(col(Dic.colVideoTagList)).as(Dic.colInPackageTagPreference)
      )
    val play_medias_part_74=play_medias_part_74_temp
      .withColumn(Dic.colInPackageVideoTwoLevelPreference,udfGetLabelAndCount2(col(Dic.colInPackageVideoTwoLevelPreference)))
      .withColumn(Dic.colInPackageTagPreference,udfGetLabelAndCount2(col(Dic.colInPackageTagPreference)))



    val df_user_profile_pref = df_user_profile_pref3.join(play_medias_part_71,joinKeysUserId,"left")
      .join(play_medias_part_72,joinKeysUserId, "left")
      .join(play_medias_part_73,joinKeysUserId,"left")
      .join(play_medias_part_74,joinKeysUserId, "left")


    df_user_profile_pref








  }



}
