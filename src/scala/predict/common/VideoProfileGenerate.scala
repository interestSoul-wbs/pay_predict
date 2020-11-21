package predict.common

import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.github.nscala_time.time.Imports._
import scala.collection.mutable.ListBuffer

object VideoProfileGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var sixteenDaysAgo: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    // 测试集的划分时间点 - 例：2020-09-15 00:00:00， 截止日期是 2020-10-01
    sixteenDaysAgo = (date - 16.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 1 - processed df_medias
    val df_medias = getProcessedMedias(spark, partitiondate, license)

    printDf("df_medias", df_medias)

    val selectColumns = df_medias.columns

    val df_medias_purged = df_medias
      .na.drop(Array(Dic.colVideoId, Dic.colReleaseDate, Dic.colStorageTime, Dic.colVideoTime))
      .withColumn(Dic.colIsOnlyNumberVideoId, udfIsOnlyNumber(col(Dic.colVideoId)))
      .withColumn(Dic.colIsForMattedTimeReleaseDate, udfIsFormattedTime(col(Dic.colReleaseDate)))
      .withColumn(Dic.colIsForMattedTimeStorageTime, udfIsFormattedTime(col(Dic.colStorageTime)))
      .withColumn(Dic.colIsOnlyNumberVideoTime, udfIsOnlyNumber(col(Dic.colVideoTime).cast(IntegerType)))
      .filter(
        col(Dic.colIsOnlyNumberVideoId).===(1)
          && col(Dic.colIsForMattedTimeReleaseDate).===(1)
          && col(Dic.colIsForMattedTimeStorageTime).===(1)
          && col(Dic.colIsOnlyNumberVideoTime).===(1))
      .select(selectColumns.head, selectColumns.tail: _*)

    printDf("df_medias_purged", df_medias_purged)

    // 2 - processed play data
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    printDf("df_plays", df_plays)

    // 3 - processed order data
    val df_orders = getProcessedOrder(spark, partitiondate, license)

    printDf("df_orders", df_orders)

    // 4 - data process
    val df_result = videoProfileGenerate(sixteenDaysAgo, 30, df_medias_purged, df_plays, df_orders)

    printDf("df_result", df_result)

    // 5 - save data
    saveVideoProfileGenerate(spark, df_result, partitiondate, license, "valid")
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

    printDf("part_11", part_11)

    val part_12 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn14Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin14Days))

    printDf("part_12", part_12)

    val part_13 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn7Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin7Days))

    printDf("part_13", part_13)

    val part_14 = df_plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3))
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn3Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin3Days))

    printDf("part_14", part_14)

    val part_15 = df_medias
      .withColumn(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent, udfGetDays(col(Dic.colStorageTime), lit(now)))
      .select(col(Dic.colVideoId), col(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))


    printDf("part_15", part_15)

    val df_result_tmp_1 = df_medias
      .join(part_11, joinKeysVideoId, "left")
      .join(part_12, joinKeysVideoId, "left")
      .join(part_13, joinKeysVideoId, "left")
      .join(part_14, joinKeysVideoId, "left")
      .join(part_15, joinKeysVideoId, "left")

    printDf("df_result_tmp_1", df_result_tmp_1)

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

    printDf("part_21", part_21)

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

    printDf("part_22", part_22)

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

    printDf("part_23", part_23)

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

    printDf("part_24", part_24)

    val part_25 = df_orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedTotal))
      .select(
        col(Dic.colResourceId).as(Dic.colVideoId),
        col(Dic.colNumberOfTimesPurchasedTotal))

    printDf("part_25", part_25)

    printDf("df_result_tmp_1", df_result_tmp_1)

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

    val df_result = df_result_tmp_2.na.fill(0, numColumns)

    printDf("df_result", df_result)

    df_result
  }

  /**
    * Save data.
    *
    * @param spark
    * @param df_result
    */
  def saveVideoProfileGenerate(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_video_profile(
        |            video_id string,
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
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_video_profile
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
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
