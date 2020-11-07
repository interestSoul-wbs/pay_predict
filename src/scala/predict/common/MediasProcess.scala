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
import mam.GetSaveData._

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
    // 2020-11-3 - Konverse - 上线时，partitiondate 需根据时间进行修改
    val df_raw_media = getRawMediaData(spark, "20201028", license)
    printDf("df_raw_media", df_raw_media)

    // 3 - media data process
    val df_media = mediaDataProcess(df_raw_media)

    df_raw_media.unpersist()
    printDf("df_media", df_media)

    // 4 - Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList
    // extract tags and save
    getSingleStrColLabelAndSave(spark, df_media, Dic.colVideoOneLevelClassification, Dic.colOneLevel)

    getArrayStrColLabelAndSave(spark, df_media, Dic.colVideoTwoLevelClassificationList, Dic.colTwoLevel)

    getArrayStrColLabelAndSave(spark, df_media, Dic.colVideoTagList, Dic.colVideoTag)

    // 5 - Fill Dic.colScore, Dic.colVideoTime Na with MEAN value
    val cols = Array(Dic.colScore, Dic.colVideoTime)
    val df_media_processed = fillNaWithMean(df_media, cols)

    printDf("df_media_processed", df_media_processed)

    // 6 - save processed media
    saveProcessedMedia(spark, df_media_processed, "20201028", license)

    println("预测阶段媒资数据处理完成！")
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
  def getSingleStrColLabelAndSave(spark: SparkSession, df_media: DataFrame, col_name: String, category: String) = {

    val df_label = df_media
      .select(col(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(spark, df_label, partitiondate, license, category)
  }

  /**
    * * Process video_two_level_classification_list and video_tag_list, save data to hive
    *
    * @param df_media
    * @param col_name
    * @param category
    */
  def getArrayStrColLabelAndSave(spark: SparkSession, df_media: DataFrame, col_name: String, category: String) = {

    val df_label = df_media
      .select(
        explode(
          col(col_name)).as(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).cast(StringType).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(spark, df_label, partitiondate, license, category)
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
