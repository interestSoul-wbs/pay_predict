package train.common

/**
  * @Author wj
  * @Date 2020/09
  * @Version 1.0
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
    // 2020-11-3 - Konverse - 上线时，partitiondate 需根据时间进行修改
    val df_raw_media = getRawMediaData(spark, partitiondate, license)
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
    saveProcessedMedia(spark, df_media_processed, partitiondate, license)

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


  /**
    * Save processed media data to hive
    *
    * @param df_media
    */
  def saveProcessedMedia(spark: SparkSession, df_media: DataFrame, partitiondate: String, license: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_media(
        |            video_id            	string,
        |            video_title         	string,
        |            video_one_level_classification	string,
        |            video_two_level_classification_list	array<string>,
        |            video_tag_list      	array<string>,
        |            director_list       	array<string>,
        |            actor_list          	array<string>,
        |            country             	string,
        |            language            	string,
        |            release_date        	string,
        |            storage_time        	string,
        |            video_time          	double,
        |            score               	double,
        |            is_paid             	double,
        |            package_id          	string,
        |            is_single           	double,
        |            is_trailers         	double,
        |            supplier            	string,
        |            introduction        	string)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_media.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_media
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
    * Save the tag of video_one_level_classification, video_two_level_classification_list, video_tag_list
    *
    * @param df_label
    * @param category
    */
  def saveLabel(spark: SparkSession, df_label: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_media_label(
        |            content            	string)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_label.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_media_label
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




}
