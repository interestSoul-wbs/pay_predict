package train.common

import mam.Dic
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._
import scala.collection.mutable.ListBuffer

object VideoProfileGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-06-01 00:00:00
    val now = "2020-09-01 00:00:00"

    // 1 - processed df_medias
    val df_medias = getProcessedMedias(spark, partitiondate, license)

    printDf("df_medias", df_medias)

    // 2 - processed play data
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    printDf("df_plays", df_plays)

    // 3 - processed order data
    val df_orders = getProcessedOrder(spark, partitiondate, license)

    printDf("df_orders", df_orders)

    // 4 - data process
    val df_result = videoProfileGenerate(now, 30, df_medias, df_plays, df_orders)

    printDf("df_result", df_result)

    // 5 - save data
    saveData(spark, df_result)
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
        |     vodrs.t_sdu_user_video_profile_paypredict(
        |video_id string,
        |            video_title string,
        |            video_one_level_classification string,
        |            video_two_level_classification_list array<string>,
        |            video_tag_list array<string>,
        |            director_list array<string>,
        |            actor_list array<string>,
        |            country string,
        |            language string,
        |            release_date string,
        |            storage_time string,
        |            video_time double,
        |            score double,
        |            is_paid double,
        |            package_id string,
        |            is_single double,
        |            is_trailers double,
        |            supplier string,
        |            introduction string,
        |            number_of_plays_in_30_days long,
        |            number_of_views_within_30_days long,
        |            number_of_plays_in_14_days long,
        |            number_of_views_within_14_days long,
        |            number_of_plays_in_7_days long,
        |            number_of_views_within_7_days long,
        |            number_of_plays_in_3_days long,
        |            number_of_views_within_3_days long,
        |            abs_of_number_of_days_between_storage_and_current integer,
        |            number_of_times_purchased_within_30_days long,
        |            number_of_times_purchased_within_14_days long,
        |            number_of_times_purchased_within_7_days long,
        |            number_of_times_purchased_within_3_days long,
        |            number_of_times_purchased_total long)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_sdu_user_video_profile_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    video_id,
         |    video_title,
         |    video_one_level_classification,
         |    video_two_level_classification_list,
         |    video_tag_list,
         |    director_list,
         |    actor_list,
         |    country,
         |    language,
         |    release_date,
         |    storage_time,
         |    video_time,
         |    score,
         |    is_paid,
         |    package_id,
         |    is_single,
         |    is_trailers,
         |    supplier,
         |    introduction,
         |    number_of_plays_in_30_days,
         |    number_of_views_within_30_days,
         |    number_of_plays_in_14_days,
         |    number_of_views_within_14_days,
         |    number_of_plays_in_7_days,
         |    number_of_views_within_7_days,
         |    number_of_plays_in_3_days,
         |    number_of_views_within_3_days,
         |    abs_of_number_of_days_between_storage_and_current,
         |    number_of_times_purchased_within_30_days,
         |    number_of_times_purchased_within_14_days,
         |    number_of_times_purchased_within_7_days,
         |    number_of_times_purchased_within_3_days,
         |    number_of_times_purchased_total
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
