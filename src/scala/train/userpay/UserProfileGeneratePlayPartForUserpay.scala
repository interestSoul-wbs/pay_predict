package train.userpay

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, getTrainUser, saveProcessedData, saveUserProfilePlayPart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetDays}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object UserProfileGeneratePlayPartForUserpay {

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
    val df_user_profile_play = userProfileGeneratePlayPart(now, df_plays, df_train_users, df_medias)

    // 4 Save Data
    saveUserProfilePlayPart(now, df_user_profile_play, "train")
    printDf("输出 df_user_profile_play", df_user_profile_play)

    println("用户画像play部分生成完毕。")



  }

  def userProfileGeneratePlayPart(now: String, df_plays:DataFrame, df_train_users:DataFrame, df_medias:DataFrame):DataFrame = {


    val df_train_id = df_train_users.select(Dic.colUserId)
    val df_train_plays = df_plays.join(df_train_id, Seq(Dic.colUserId), "inner")
      .withColumn(Dic.colPlayDate, col(Dic.colPlayStartTime).substr(1, 10))

    printDf("df_train_plays", df_train_plays)


    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)


    /**
     * 用户画像play part 时间类相关信息
     * 时长类转换成分钟
     */
    val df_play_part_1 = df_train_plays
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast30Days),
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeLast30Days),
        udfGetDays(max(col(Dic.colPlayStartTime)), lit(now)).as(Dic.colDaysFromLastActive),
        udfGetDays(min(col(Dic.colPlayStartTime)), lit(now)).as(Dic.colDaysSinceFirstActiveInTimewindow))
      .withColumn(Dic.colTotalTimeLast30Days, round(col(Dic.colTotalTimeLast30Days) / 60, 0))


    val df_play_part_2 = df_train_plays
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast14Days),
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeLast14Days)
      ).withColumn(Dic.colTotalTimeLast14Days, round(col(Dic.colTotalTimeLast14Days) / 60, 0))


    val df_play_part_3 = df_train_plays
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast7Days),
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeLast7Days)
      )
      .withColumn(Dic.colTotalTimeLast7Days, round(col(Dic.colTotalTimeLast7Days) / 60, 0))

    val df_play_part_4 = df_train_plays
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3))
      .groupBy(col(Dic.colUserId))
      .agg(
        countDistinct(col(Dic.colPlayDate)).as(Dic.colActiveDaysLast3Days),
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeLast3Days)
      )
      .withColumn(Dic.colTotalTimeLast3Days, round(col(Dic.colTotalTimeLast3Days) / 60, 0))

    val joinKeysUserId = Seq(Dic.colUserId)
    val df_play_time = df_train_id.join(df_play_part_1, joinKeysUserId, "left")
      .join(df_play_part_2, joinKeysUserId, "left")
      .join(df_play_part_3, joinKeysUserId, "left")
      .join(df_play_part_4, joinKeysUserId, "left")

    /**
     * 付费类视频的播放相关信息
     */
    val joinKeyVideoId = Seq(Dic.colVideoId)
    val df_train_medias = df_train_plays.join(df_medias, joinKeyVideoId, "inner")

    val df_play_medias_part_11 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast30Days, round(col(Dic.colTotalTimePaidVideosLast30Days) / 60, 0))

    val df_play_medias_part_12 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast14Days, round(col(Dic.colTotalTimePaidVideosLast14Days) / 60, 0))

    val df_play_medias_part_13 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast7Days, round(col(Dic.colTotalTimePaidVideosLast7Days) / 60, 0))

    val df_play_medias_part_14 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast3Days, round(col(Dic.colTotalTimePaidVideosLast3Days) / 60, 0))

    val df_play_medias_part_15 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimePaidVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimePaidVideosLast1Days, round(col(Dic.colTotalTimePaidVideosLast1Days) / 60, 0))

   //新添加的属性，观看非付费视频的次数
    val df_play_medias_part_16=df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
        && col(Dic.colPlayStartTime).>=(pre_30)
        && col(Dic.colIsPaid).===(0)
      ).groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberOfUnpaid)
      )
    //新添加的属性，观看付费视频的次数
    val df_play_medias_part_17=df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colIsPaid).===(1)
      ).groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberOfPaid)
      )





    val df_play_medias = df_play_time.join(df_play_medias_part_11, joinKeysUserId, "left")
      .join(df_play_medias_part_12, joinKeysUserId, "left")
      .join(df_play_medias_part_13, joinKeysUserId, "left")
      .join(df_play_medias_part_14, joinKeysUserId, "left")
      .join(df_play_medias_part_15, joinKeysUserId, "left")
      //新添加的两个属性
//      .join(df_play_medias_part_16, joinKeysUserId, "left")
//      .join(df_play_medias_part_17, joinKeysUserId, "left")

    /**
     * 套餐内
     */
    val df_play_medias_part_21 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeInPackageVideosLast30Days),
        stddev(col(Dic.colTimeSum)).as(Dic.colVarTimeInPackageVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast30Days, round(col(Dic.colTotalTimeInPackageVideosLast30Days) / 60, 0))

    val df_play_medias_part_22 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeInPackageVideosLast14Days),
        stddev(col(Dic.colTimeSum)).as(Dic.colVarTimeInPackageVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast14Days, round(col(Dic.colTotalTimeInPackageVideosLast14Days) / 60, 0))

    val df_play_medias_part_23 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeInPackageVideosLast7Days),
        stddev(col(Dic.colTimeSum)).as(Dic.colVarTimeInPackageVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast7Days, round(col(Dic.colTotalTimeInPackageVideosLast7Days) / 60, 0))

    val df_play_medias_part_24 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeInPackageVideosLast3Days),
        stddev(col(Dic.colTimeSum)).as(Dic.colVarTimeInPackageVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast3Days, round(col(Dic.colTotalTimeInPackageVideosLast3Days) / 60, 0))

    val df_play_medias_part_25 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && !isnan(col(Dic.colPackageId)))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeInPackageVideosLast1Days),
        stddev(col(Dic.colTimeSum)).as(Dic.colVarTimeInPackageVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberInPackagesVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimeInPackageVideosLast1Days, round(col(Dic.colTotalTimeInPackageVideosLast1Days) / 60, 0))

    val df_medias_play = df_play_medias.join(df_play_medias_part_21, joinKeysUserId, "left")
      .join(df_play_medias_part_22, joinKeysUserId, "left")
      .join(df_play_medias_part_23, joinKeysUserId, "left")
      .join(df_play_medias_part_24, joinKeysUserId, "left")
      .join(df_play_medias_part_25, joinKeysUserId, "left")


    val df_play_medias_part_31 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeChildrenVideosLast30Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast30Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast30Days, round(col(Dic.colTotalTimeChildrenVideosLast30Days) / 60, 0))

    val df_play_medias_part_32 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeChildrenVideosLast14Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast14Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast14Days, round(col(Dic.colTotalTimeChildrenVideosLast14Days) / 60, 0))

    val df_play_medias_part_33 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeChildrenVideosLast7Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast7Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast7Days, round(col(Dic.colTotalTimeChildrenVideosLast7Days) / 60, 0))

    val df_play_medias_part_34 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeChildrenVideosLast3Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast3Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast3Days, round(col(Dic.colTotalTimeChildrenVideosLast3Days) / 60, 0))

    val df_play_medias_part_35 = df_train_medias
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("幼儿"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colTimeSum)).as(Dic.colTotalTimeChildrenVideosLast1Days),
        countDistinct(col(Dic.colVideoId)).as(Dic.colNumberChildrenVideosLast1Days)
      )
      .withColumn(Dic.colTotalTimeChildrenVideosLast1Days, round(col(Dic.colTotalTimeChildrenVideosLast1Days) / 60, 0))


    val df_user_profile_play = df_medias_play.join(df_play_medias_part_31, joinKeysUserId, "left")
      .join(df_play_medias_part_32, joinKeysUserId, "left")
      .join(df_play_medias_part_33, joinKeysUserId, "left")
      .join(df_play_medias_part_34, joinKeysUserId, "left")
      .join(df_play_medias_part_35, joinKeysUserId, "left")


    df_user_profile_play

  }


}
