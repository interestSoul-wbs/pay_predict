package predict.common

/**
  * @Author wj
  * @Date 2020/09
  * @Version 1.0
  */

import mam.Dic
import mam.Utils.{generateSparkSession, printArray, printDf, udfLongToTimestamp}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object MediasProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    // 1 - SparkSession and params initialize
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    partitiondate = args(0)
    license = args(1)

    // 2 - get raw media data
    val df_raw_media = getMediaData(spark)
    printDf("df_raw_media", df_raw_media)

    // 3 - media data process
    val df_media = mediaDataProcess(df_raw_media)

    df_raw_media.unpersist()
    printDf("df_media", df_media)

    // 4 - Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList
    // extract tags and save
    getSingleStrColLabelAndSave(df_media, Dic.colVideoOneLevelClassification, Dic.colOneLevel, spark)

    getArrayStrColLabelAndSave(df_media, Dic.colVideoTwoLevelClassificationList, Dic.colTwoLevel, spark)

    getArrayStrColLabelAndSave(df_media, Dic.colVideoTagList, Dic.colVideoTag, spark)

    // 5 - Fill Dic.colScore, Dic.colVideoTime Na with MEAN value
    val cols = Array(Dic.colScore, Dic.colVideoTime)
    val df3 = fillNaWithMean(df_media, cols)

    printDf("df3", df3)

    // 6 - save
    saveProcessedMedia(df3, spark)

    println("预测阶段媒资数据处理完成！")
  }

  /**
    * 从 t_media_sum 获取 物品的数据
    */
  def getMediaData(spark: SparkSession) = {
    val get_result_sql =
      s"""
         |SELECT
         |    media_id as video_id,
         |    title as video_title,
         |    type_name as video_one_level_classification,
         |    category_name_array as video_two_level_classification_list,
         |    tag_name_array as video_tag_list,
         |    director_name_array as director_list,
         |    actor_name_array as actor_list,
         |    country as country,
         |    language as language,
         |    pubdate as release_date,
         |    created_time as storage_time,
         |    cast(time_length as double) video_time,
         |    cast(rate as double) score,
         |    cast(fee as double) is_paid,
         |    cast(vip_id as string) package_id,
         |    cast(is_single as double) is_single,
         |    cast(is_clip as double) is_trailers,
         |    vender_name as supplier,
         |    summary as introduction
         |FROM
         |    vodrs.t_media_sum
         |WHERE
         |    partitiondate = '$partitiondate' and
         |    license = '$license'
       """.stripMargin

    val df_result = spark.sql(get_result_sql)

    df_result
  }

  /**
    * Save the tag of video_one_level_classification, video_two_level_classification_list, video_tag_list
    *
    * @param df_label
    * @param category
    */
  def saveLabel(df_label: DataFrame, category: String, spark: SparkSession) = {

    println("save data to hive........... \n" * 4)
    df_label.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_media_label_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license', category = '$category')
         |SELECT
         |    content
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  /**
    * Save processed media data to hive
    *
    * @param df_media
    */
  def saveProcessedMedia(df_media: DataFrame, spark: SparkSession) = {

    println("save data to hive........... \n" * 4)
    df_media.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_media_sum_processed_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    video_id ,
         |    video_title ,
         |    video_one_level_classification ,
         |    video_two_level_classification_list ,
         |    video_tag_list ,
         |    director_list ,
         |    actor_list ,
         |    country ,
         |    language ,
         |    release_date ,
         |    storage_time ,
         |    video_time ,
         |    score ,
         |    is_paid ,
         |    package_id ,
         |    is_single ,
         |    is_trailers ,
         |    supplier ,
         |    introduction
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  /**
    * Process of media data
    *
    * @param df_raw_media
    * @return
    */
  def mediaDataProcess(df_raw_media: DataFrame) = {

    val df_media = df_raw_media
      .withColumn("tmp", udfLongToTimestamp(col(Dic.colStorageTime)))
      .drop(Dic.colStorageTime)
      .withColumnRenamed("tmp", Dic.colStorageTime)

    df_media
  }

  /**
    * Process video_one_level_classification and save data to hive
    *
    * @param df_media
    * @param col_name
    * @param category
    */
  def getSingleStrColLabelAndSave(df_media: DataFrame, col_name: String, category: String, spark: SparkSession) = {

    val df_label = df_media
      .select(col(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(df_label, category, spark)
  }

  /**
    * * Process video_two_level_classification_list and video_tag_list, save data to hive
    *
    * @param df_media
    * @param col_name
    * @param category
    */
  def getArrayStrColLabelAndSave(df_media: DataFrame, col_name: String, category: String, spark: SparkSession) = {

    val df_label = df_media
      .select(
        explode(
          col(col_name)).as(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).cast(StringType).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(df_label, category, spark)
  }

  /**
    * fill score, video_time with mean value
    *
    * @param df_media
    * @param cols
    * @return
    */
  def fillNaWithMean(df_media: DataFrame, cols: Array[String]) = {

    val imputer = new Imputer()
      .setInputCols(cols)
      .setOutputCols(cols)
      .setStrategy("mean")

    val df_result = imputer.fit(df_media).transform(df_media)

    df_result
  }
}
