package train.common

import mam.Utils.{calDate, udfGetLabelAndCount, udfGetLabelAndCount2}
import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserProfileGeneratePreferencePart {

  def userProfileGeneratePreferencePart(now:String,timeWindow:Int,medias_path:String,plays_path:String,orders_path:String,hdfsPath:String): Unit = {
     System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGeneratePreferencePart")
      .master("local[6]")
      .getOrCreate()
    //设置shuffle过程中分区数
    // spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1000")
    import org.apache.spark.sql.functions._

    val medias = spark.read.format("parquet").load(medias_path)
    val plays = spark.read.format("parquet").load(plays_path)
    val orders = spark.read.format("parquet").load(orders_path)


    val result = plays.select(col(Dic.colUserId)).distinct()



    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeyVideoId=Seq(Dic.colVideoId)
    val user_medias=plays.join(medias,joinKeyVideoId,"inner")


    val play_medias_part_41=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast30Days)
      )
    val play_medias_part_42=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast14Days)
      )
    val play_medias_part_43=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast7Days)
      )
    val play_medias_part_44=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast3Days)
      )
    val play_medias_part_45=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影"))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimeMoviesLast1Days)
      )

    val result20=result.join(play_medias_part_41,joinKeysUserId,"left")
    val result21=result20.join(play_medias_part_42,joinKeysUserId, "left")
    val result22=result21.join(play_medias_part_43,joinKeysUserId,"left")
    val result23=result22.join(play_medias_part_44,joinKeysUserId, "left")
    val result24=result23.join(play_medias_part_45,joinKeysUserId, "left")



    val play_medias_part_51=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast30Days)
      )
    val play_medias_part_52=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast14Days)
      )
    val play_medias_part_53=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast7Days)
      )
    val play_medias_part_54=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast3Days)
      )
    val play_medias_part_55=user_medias
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_1)
          && col(Dic.colVideoOneLevelClassification).===("电影")
          && col(Dic.colIsPaid).===(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colTotalTimePaidMoviesLast1Days)
      )
    val result25=result24.join(play_medias_part_51,joinKeysUserId,"left")
    val result26=result25.join(play_medias_part_52,joinKeysUserId, "left")
    val result27=result26.join(play_medias_part_53,joinKeysUserId,"left")
    val result28=result27.join(play_medias_part_54,joinKeysUserId, "left")
    val result29=result28.join(play_medias_part_55,joinKeysUserId, "left")



    val play_medias_part_61=user_medias
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
    val play_medias_part_62=user_medias
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
    val play_medias_part_63=user_medias
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
    val play_medias_part_64=user_medias
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
    val result30=result29.join(play_medias_part_61,joinKeysUserId,"left")
    val result31=result30.join(play_medias_part_62,joinKeysUserId, "left")
    val result32=result31.join(play_medias_part_63,joinKeysUserId,"left")
    val result33=result32.join(play_medias_part_64,joinKeysUserId, "left")





    val play_medias_part_71_temp=user_medias
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


    val play_medias_part_72_temp=user_medias
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



    val play_medias_part_73_temp=user_medias
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

    val play_medias_part_74_temp=user_medias
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



        val result34=result33.join(play_medias_part_71,joinKeysUserId,"left")
        val result35=result34.join(play_medias_part_72,joinKeysUserId, "left")
        val result36=result35.join(play_medias_part_73,joinKeysUserId,"left")
        val result37=result36.join(play_medias_part_74,joinKeysUserId, "left")


    val userProfilePreferencePartSavePath=hdfsPath+"data/train/common/processed/userprofilepreferencepart"+now.split(" ")(0)
    //大约有85万用户
    result37.write.mode(SaveMode.Overwrite).format("parquet").save(userProfilePreferencePartSavePath)







  }


  def main(args:Array[String]): Unit ={
    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath=""
    val mediasProcessedPath=hdfsPath+"data/train/common/processed/mediastemp"
    val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
    val now=args(0)+" "+args(1)
    userProfileGeneratePreferencePart(now,30,mediasProcessedPath,playsProcessedPath,ordersProcessedPath,hdfsPath)


  }

}
