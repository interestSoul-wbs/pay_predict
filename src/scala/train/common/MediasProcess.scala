package train.common

/**
  * Konverse - 2020-11-30
  */

import mam.Dic
import mam.Utils._
import mam.GetSaveData._
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    val df_raw_media = getRawMediaData(partitiondate, license)

    // 3 - media data process
    val df_media = mediaDataProcess(df_raw_media)

    df_raw_media.unpersist()

    // 4 - Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList
    // extract tags and save
    getSingleStrColLabelAndSave(df_media, Dic.colVideoOneLevelClassification, Dic.colOneLevel)

    getArrayStrColLabelAndSave(df_media, Dic.colVideoTwoLevelClassificationList, Dic.colTwoLevel)

    getArrayStrColLabelAndSave(df_media, Dic.colVideoTagList, Dic.colVideoTag)

    // 5 - Fill Dic.colScore, Dic.colVideoTime Na with MEAN value
    val cols = Array(Dic.colScore, Dic.colVideoTime)
    val df_media_processed = fillNaWithMean(df_media, cols)

    // 6 - save processed media
    saveProcessedMedia(df_media_processed, partitiondate, license)

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
      .na.drop(Array(Dic.colVideoId, Dic.colReleaseDate, Dic.colStorageTime, Dic.colVideoTime))
      .withColumn(Dic.colIsOnlyNumberVideoId, udfIsOnlyNumber(col(Dic.colVideoId)))
      .withColumn(Dic.colIsForMattedTimeReleaseDate, udfIsFormattedTime(col(Dic.colReleaseDate)))
      .withColumn(Dic.colIsLongtypeTimeStorageTime, udfIsLongTypeTimePattern1(col(Dic.colStorageTime)))
      .withColumn(Dic.colIsOnlyNumberVideoTime, udfIsOnlyNumber(col(Dic.colVideoTime).cast(IntegerType)))
      .filter(
        col(Dic.colIsOnlyNumberVideoId).===(1)
          && col(Dic.colIsForMattedTimeReleaseDate).===(1)
          && col(Dic.colIsLongtypeTimeStorageTime).===(1)
          && col(Dic.colIsOnlyNumberVideoTime).===(1))
      .select(
        col(Dic.colVideoId).as(Dic.colVideoId),
        col(Dic.colVideoTitle),
        col(Dic.colVideoOneLevelClassification),
        col(Dic.colVideoTwoLevelClassificationList),
        col(Dic.colVideoTagList),
        col(Dic.colDirectorList),
        col(Dic.colCountry),
        col(Dic.colActorList),
        col(Dic.colLanguage),
        col(Dic.colReleaseDate),
        udfLongToTimestamp(col(Dic.colStorageTime)).as(Dic.colStorageTime),
        col(Dic.colVideoTime),
        col(Dic.colScore),
        col(Dic.colIsPaid),
        col(Dic.colPackageId),
        col(Dic.colIsSingle),
        col(Dic.colIsTrailers),
        col(Dic.colSupplier),
        col(Dic.colIntroduction))

    df_media
  }

  /**
    * Process video_one_level_classification and save data to hive
    */
  def getSingleStrColLabelAndSave(df_media: DataFrame, col_name: String, category: String) = {

    val df_label = df_media
      .select(col(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(df_label, partitiondate, license, category)
  }

  /**
    * * Process video_two_level_classification_list and video_tag_list, save data to hive
    *
    * @param df_media
    * @param col_name
    * @param category
    */
  def getArrayStrColLabelAndSave(df_media: DataFrame, col_name: String, category: String) = {

    val df_label = df_media
      .select(
        explode(
          col(col_name)).as(col_name))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(col_name))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(col_name)).cast(StringType).as(Dic.colContent))

    printDf("df_label", df_label)

    saveLabel(df_label, partitiondate, license, category)
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
