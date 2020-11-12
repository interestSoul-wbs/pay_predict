package train.common

import mam.Dic
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer


object VideoProfileGenerate {

  def main(args:Array[String]): Unit = {

    val hdfsPath = "hdfs:///pay_predict/"

    val mediasProcessedPath = hdfsPath+"data/train/common/processed/mediastemp"
    val playsProcessedPath = hdfsPath+"data/train/common/processed/plays"
    val ordersProcessedPath = hdfsPath+"data/train/common/processed/orders"

    System.setProperty("hadoop.home.dir", "c:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("VideoProfileGenerate")
      //.master("local[6]")
      .getOrCreate()
    //设置shuffle过程中分区数
    // spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1000")

    val df_medias = spark.read.format("parquet").load(mediasProcessedPath)

    printDf("df_medias", df_medias)

    val df_plays = spark.read.format("parquet").load(playsProcessedPath)

    printDf("df_plays", df_plays)

    val df_orders = spark.read.format("parquet").load(ordersProcessedPath)

    printDf("df_orders", df_orders)

    val now=args(0)+" "+args(1)

    val df_result = videoProfileGenerate(now, 30, df_medias, df_plays, df_orders)

    val videoProfilePath=hdfsPath+"data/train/common/processed/videoprofile"+now.split(" ")(0)

    df_result.write.mode(SaveMode.Overwrite).format("parquet").save(videoProfilePath)
  }

  def videoProfileGenerate(now: String, timeWindow: Int, df_medias: DataFrame, df_plays: DataFrame, df_orders: DataFrame) = {

    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)

    val joinKeysVideoId = Seq(Dic.colVideoId)

    val part_11 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn30Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin30Days))

    val part_12 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn14Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin14Days))

    val part_13 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn7Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin7Days))

    val part_14 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn3Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin3Days))

    val part_15 = df_medias
      .withColumn(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent, udfGetDays(col(Dic.colStorageTime), lit(now)))
      .select(col(Dic.colVideoId), col(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))

    val df_result_tmp_1 = df_medias
      .join(part_11, joinKeysVideoId, "left")
      .join(part_12, joinKeysVideoId, "left")
      .join(part_13, joinKeysVideoId, "left")
      .join(part_14, joinKeysVideoId, "left")
      .join(part_15, joinKeysVideoId, "left")

    val part_21 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_30)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin30Days))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedWithin30Days))

    val part_22 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_14)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin14Days))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedWithin14Days))

    val part_23 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_7)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin7Days))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedWithin7Days))

    val part_24 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_3)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin3Days))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedWithin3Days))

    val part_25 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedTotal))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedTotal))

    val df_result_tmp_2 = df_result_tmp_1
      .join(part_21, Seq(Dic.colVideoId), "left")
      .join(part_22, Seq(Dic.colVideoId), "left")
      .join(part_23, Seq(Dic.colVideoId), "left")
      .join(part_24, Seq(Dic.colVideoId), "left")
      .join(part_25, Seq(Dic.colVideoId), "left")

    printDf("df_result_tmp_2", df_result_tmp_2)

    //选出数据类型为数值类型的列
    val numColumns = new ListBuffer[String]
    for (elem <- df_result_tmp_2.dtypes) {
      if (elem._2.equals("DoubleType") || elem._2.equals("LongType") || elem._2.equals("IntegerType")) {
        numColumns.insert(numColumns.length, elem._1)
      }
    }

    val df_result_tmp_3 = df_result_tmp_2.na.fill(0, numColumns)

    printDf("df_result_tmp_3", df_result_tmp_3)

    //将其他类型的列转化为字符串，容易保存为csv文件
    val anoColumns = df_result_tmp_3.columns.diff(numColumns)

    val df_result = anoColumns.foldLeft(df_result_tmp_2) {
      (currentDF, column) => currentDF.withColumn(column, col(column).cast("string"))
    }

    df_result
  }

}
