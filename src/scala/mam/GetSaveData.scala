package mam

import mam.Utils.udfVectorToArray
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * @author wj
 * @date 2020/11/23 ${Time}
 * @version 0.1
 * @describe
 */
object GetSaveData {

  def getRawPlays(spark:SparkSession,playRawPath:String)={
    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)
      )
    )
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(playRawPath)
    df

  }

  /**
   * 读取原始媒资数据
   * @param spark
   * @return
   */
  def getRawMedias(spark: SparkSession,mediasRawPath:String) = {


    val schema = StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colVideoTitle, StringType),
        StructField(Dic.colVideoOneLevelClassification, StringType),
        StructField(Dic.colVideoTwoLevelClassificationList, StringType),
        StructField(Dic.colVideoTagList, StringType),
        StructField(Dic.colDirectorList, StringType),
        StructField(Dic.colActorList, StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField(Dic.colReleaseDate, StringType),
        StructField(Dic.colStorageTime, StringType),
        //视频时长
        StructField(Dic.colVideoTime, StringType),
        StructField(Dic.colScore, StringType),
        StructField(Dic.colIsPaid, StringType),
        StructField(Dic.colPackageId, StringType),
        StructField(Dic.colIsSingle, StringType),
        //是否片花
        StructField(Dic.colIsTrailers, StringType),
        StructField(Dic.colSupplier, StringType),
        StructField(Dic.colIntroduction, StringType)
      )
    )
    val dfRawMedias = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(mediasRawPath)

    dfRawMedias
  }

  /**
   * 读取原始订单数据
   * @param orderRawPath
   * @param spark
   * @return
   */
  def getRawOrders(spark:SparkSession,orderRawPath:String)={
    val schema= StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colMoney, StringType),
        StructField(Dic.colResourceType, StringType),
        StructField(Dic.colResourceId, StringType),
        StructField(Dic.colResourceTitle, StringType),
        StructField(Dic.colCreationTime, StringType),
        StructField(Dic.colDiscountDescription, StringType),
        StructField(Dic.colOrderStatus, StringType),
        StructField(Dic.colOrderStartTime, StringType),
        StructField(Dic.colOrderEndTime, StringType)

      )
    )
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(orderRawPath)
    df

  }






  /**
   * Get user order data.
   *
   * @param spark
   * @return
   */
  def getProcessedOrders(spark: SparkSession,orderProcessedPath:String) = {
    spark.read.format("parquet").load(orderProcessedPath).toDF()
  }

  /**
   * Get processed order
   * @param spark
   * @param playProcessedPath
   */
  def getProcessedPlays(spark: SparkSession,playProcessedPath:String)={
    spark.read.format("parquet").load(playProcessedPath).toDF()
  }

  def getProcessedMedias(spark: SparkSession,mediaProcessedPath:String)={
    spark.read.format("parquet").load(mediaProcessedPath).toDF()
  }

  case class splitVecToDoubleDfAndNewCols(df_result_with_vec_cols: DataFrame, new_cols_array: Array[String])

  /**
   *
   * @param df_raw_video_dict
   * @return
   */
  def splitVecToDouble(df_raw_video_dict: DataFrame, vec_col: String, vectorDimension: Int, prefix_of_separate_vec_col: String) = {

    // 将 vec_col 切分后的df
    val df_result = df_raw_video_dict
      .select(
        col("*")
          +: (0 until vectorDimension).map(i => col(vec_col).getItem(i).as(s"$prefix_of_separate_vec_col" + s"_$i")): _*)

    // 将 vec_col 切分后，新出现的 col name array
    val new_cols_array = ArrayBuffer[String]()

    for (i <- 0 until vectorDimension) {
      new_cols_array.append(s"$prefix_of_separate_vec_col" + s"_$i")
    }

    splitVecToDoubleDfAndNewCols(df_result, new_cols_array.toArray)
  }

  /**
   *
   * @param df_original
   * @param excluded_cols
   * @return
   */
  def scaleData(df_original: DataFrame, excluded_cols: Array[String]) = {

    val all_original_cols = df_original.columns

    val input_cols = all_original_cols.diff(excluded_cols)

    val final_df_schema = excluded_cols.++(input_cols)

    val assembler = new VectorAssembler()
      .setInputCols(input_cols)
      .setOutputCol(Dic.colFeatures)

    val df_output = assembler.transform(df_original)

    val minMaxScaler = new MinMaxScaler()
      .setInputCol(Dic.colFeatures)
      .setOutputCol(Dic.colScaledFeatures)
      .fit(df_output)

    val df_scaled_data = minMaxScaler.transform(df_output)
      .withColumn(Dic.colVector, udfVectorToArray(col(Dic.colScaledFeatures)))

    val splitVecToDoubleDfAndNewCols = splitVecToDouble(df_scaled_data, Dic.colVector, input_cols.length, "f")

    val df_result = splitVecToDoubleDfAndNewCols.df_result_with_vec_cols
      .select(excluded_cols.++(splitVecToDoubleDfAndNewCols.new_cols_array).map(col): _*)
      .toDF(final_df_schema: _*)

    df_result
  }

}
