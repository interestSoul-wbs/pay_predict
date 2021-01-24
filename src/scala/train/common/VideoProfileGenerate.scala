package train.common



import mam.Utils.{calDate, printDf, sysParamSetting, udfGetDays}
import mam.{Dic, SparkSessionInit}
import mam.GetSaveData._
import mam.SparkSessionInit.spark
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object VideoProfileGenerate {
  def main(args:Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)
    val df_medias=getProcessedMedias(spark)
    printDf("输入 df_media",df_medias)
    val df_orders=getProcessedOrder(spark)
    printDf("输入 df_order",df_orders)
    val df_plays=getProcessedPlay(spark)
    printDf("输入 df_play",df_plays)

    //3 Process Data
    val df_video_profile=videoProfileGenerate(now, df_medias, df_plays, df_orders)


    //4 Save Data
    saveVideoProfile(now,df_video_profile,"train")
    printDf("输出 df_video_profile",df_video_profile)
    println("VideoProfileGenerate  over~~~~~~~~~~~")







  }

  def videoProfileGenerate(now:String,df_medias:DataFrame,df_plays:DataFrame,df_orders:DataFrame)={



    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now,  -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysVideoId = Seq(Dic.colVideoId)


    val part_11=df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn30Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin30Days)
      )
    val part_12=df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn14Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin14Days)
      )
    val part_13=df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn7Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin7Days)
      )
    val part_14=df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn3Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin3Days)
      )

    val part_15=df_medias
      .withColumn(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent,udfGetDays(col(Dic.colStorageTime),lit(now)))
      .select(col(Dic.colVideoId),col(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))



    val df_medias_play=df_medias.join(part_11,joinKeysVideoId,"left")
      .join(part_12,joinKeysVideoId,"left")
      .join(part_13,joinKeysVideoId,"left")
      .join(part_14,joinKeysVideoId,"left")
      .join(part_15,joinKeysVideoId,"left")



    val part_21=df_orders.filter(
      col(Dic.colCreationTime).<(now)
      && col(Dic.colCreationTime).>=(pre_30)
      && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
          count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin30Days)
      ).withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    val part_22=df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_14)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin14Days)
      ).withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    val part_23=df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_7)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin7Days)
      ).withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    val part_24=df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_3)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin3Days)
      ).withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    val part_25=df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedTotal)
      ).withColumnRenamed(Dic.colResourceId,Dic.colVideoId)

    val df_video_profile=df_medias_play.join(part_21,joinKeysVideoId,"left")
      .join(part_22,joinKeysVideoId,"left")
      .join(part_23,joinKeysVideoId,"left")
      .join(part_24,joinKeysVideoId,"left")
      .join(part_25,joinKeysVideoId,"left")


    //选出数据类型为数值类型的列
    val numColumns=new ListBuffer[String]
    for(elem<-df_video_profile.dtypes){
      if(elem._2.equals("DoubleType")||elem._2.equals("LongType")||elem._2.equals("IntegerType")){
          numColumns.insert(numColumns.length,elem._1)
      }
    }


    val anoColumns=df_video_profile.columns.diff(numColumns)
    val df_profile= anoColumns.foldLeft(df_video_profile){
      (currentDF, column) => currentDF.withColumn(column, col(column).cast("string"))
    }
    df_profile.na.fill(0,numColumns)

    df_profile



  }



}
