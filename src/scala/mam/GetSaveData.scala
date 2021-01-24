package mam

import mam.Utils.{getData, udfVectorToArray}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * @author wj
 * @date 2020/11/23 ${Time}
 * @version 0.1
 * @describe
 */
object GetSaveData {
  val hdfsPath="hdfs:///pay_predict/"
//  val hdfsPath=""

  def getRawPlays(spark: SparkSession) = {

    val playRawPath = hdfsPath + "data/train/common/raw/plays/*"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)))

    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(playRawPath)

    df
  }

  def getRawMediaData(spark: SparkSession) = {

    //
    val mediasRawPath = hdfsPath + "data/train/common/raw/medias/*"

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

    // Konverse - 注意 df 的命名 - df_相关属性 - 不要 dfRawMedia
    val df_raw_media = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(mediasRawPath)
      .select(
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        when(col(Dic.colVideoTitle) === "NULL", null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
        when(col(Dic.colVideoOneLevelClassification) === "NULL" or (col(Dic.colVideoOneLevelClassification) === ""), null)
          .otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
        from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
        from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
        from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
        from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
//        when(col(Dic.colVideoOneLevelClassification).isNotNull, col(Dic.colVideoOneLevelClassification)), // Konverse - 这一步 相当于缺失值填充，被移动到了 process
        when(col(Dic.colCountry) === "NULL", null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
        when(col(Dic.colLanguage) === "NULL", null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
        when(col(Dic.colReleaseDate) === "NULL", null).otherwise(col(Dic.colReleaseDate)).as(Dic.colReleaseDate),
        when(col(Dic.colStorageTime) === "NULL", null).otherwise(col(Dic.colStorageTime)).as(Dic.colStorageTime), // Konverse - 这一步的udf被移动到了 process
        when(col(Dic.colVideoTime) === "NULL", null).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
        when(col(Dic.colScore) === "NULL", null).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
        when(col(Dic.colIsPaid) === "NULL", null).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
        when(col(Dic.colPackageId) === "NULL", null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
        when(col(Dic.colIsSingle) === "NULL", null).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
        when(col(Dic.colIsTrailers) === "NULL", null).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
        when(col(Dic.colSupplier) === "NULL", null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
        when(col(Dic.colIntroduction) === "NULL", null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction))

    df_raw_media
  }

  def getRawOrders(spark: SparkSession) = {

    val orderRawPath = hdfsPath + "data/train/common/raw/orders/*"

    val schema = StructType(
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
        StructField(Dic.colOrderEndTime, StringType)))

    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(orderRawPath)

    df
  }



  def saveProcessedData(df: DataFrame, path: String) = {

    df.write.mode(SaveMode.Overwrite).format("parquet").save(path)

  }

  def saveLabel(df_label: DataFrame, fileName: String) = {
    val path = hdfsPath + "data/train/common/processed/"
    df_label.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "false").csv(path + fileName)
  }

  def saveProcessedMedia(df_processed_media: DataFrame) = {
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    saveProcessedData(df_processed_media, mediasProcessedPath)
  }
  def saveProcessedPlay(df_play_processed: DataFrame) = {

    val playProcessedPath = hdfsPath + "data/train/common/processed/plays"
    //    df_play_processed.write.mode(SaveMode.Overwrite).format("parquet").save(playProcessedPath)
    saveProcessedData(df_play_processed, playProcessedPath)
  }

  def saveProcessedOrder(df_order_processed: DataFrame) = {


    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders"
    saveProcessedData(df_order_processed, orderProcessedPath)
  }



  def getProcessedMedias(sparkSession: SparkSession) = {

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    sparkSession.read.format("parquet").load(mediasProcessedPath)

  }
  def getProcessedOrder(sparkSession: SparkSession) = {

    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders"
    sparkSession.read.format("parquet").load(ordersProcessedPath)

  }
  def getProcessedPlay(sparkSession: SparkSession) = {

    val playsProcessedPath = hdfsPath + "data/train/common/processed/plays"
    sparkSession.read.format("parquet").load(playsProcessedPath)

  }



  def saveUserProfileOrderPart(now: String, df_user_profile_order: DataFrame, state: String) = {
   //这里保存的位置和套餐付费的不一样
    val userProfileOrderPartSavePath = hdfsPath + "data/" + state + "/common/processed/userprofileorderpart" + now.split(" ")(0)
    saveProcessedData(df_user_profile_order, userProfileOrderPartSavePath)
  }
  def saveUserProfilePlayPart(now: String, df_user_profile_play: DataFrame, state: String) = {
    //这里保存的位置和套餐付费的不一样
    val userProfilePlayPartSavePath = hdfsPath + "data/" + state + "/common/processed/userprofileplaypart" + now.split(" ")(0)
    saveProcessedData(df_user_profile_play, userProfilePlayPartSavePath)
  }
  def saveUserProfilePreferencePart(now: String, df_user_profile_preference: DataFrame, state: String) = {
    //这里保存的位置和套餐付费的不一样
    val userProfilePreferencePartSavePath = hdfsPath + "data/" + state + "/common/processed/userprofilepreferencepart" + now.split(" ")(0)
    saveProcessedData(df_user_profile_preference, userProfilePreferencePartSavePath)
  }
  def getUserProfileOrderPart(sparkSession: SparkSession, now: String, state: String) = {
    val userProfileOrderPartPath = hdfsPath + "data/" + state + "/common/processed/userprofileorderpart" + now.split(" ")(0)
    getData(sparkSession, userProfileOrderPartPath)
  }

  def getUserProfilePlayPart(sparkSession: SparkSession, now: String, state: String) = {

    val userProfilePlayPartPath = hdfsPath + "data/" + state + "/common/processed/userprofileplaypart" + now.split(" ")(0)
    getData(sparkSession, userProfilePlayPartPath)
  }
  def getUserProfilePreferencePart(sparkSession: SparkSession, now: String, state: String) = {
    val userProfilePreferencePartSavePath = hdfsPath + "data/" + state + "/common/processed/userprofilepreferencepart" + now.split(" ")(0)
    getData(sparkSession, userProfilePreferencePartSavePath)
  }

  def saveVideoProfile(now:String,df_video_profile:DataFrame,state:String): Unit ={
    val videoProfilePath = hdfsPath + "data/" + state + "/common/processed/videoprofile" + now.split(" ")(0)
    saveProcessedData(df_video_profile, videoProfilePath)

  }

  def getVideoProfile(sparkSession: SparkSession, now: String, state: String)={
    val videoProfilePath = hdfsPath + "data/" + state + "/common/processed/videoprofile" + now.split(" ")(0)
    getData(sparkSession, videoProfilePath)
  }
  def saveVideoVector(now:String,df_video_vector:DataFrame,state:String): Unit ={
    val videoProfilePath = hdfsPath + "data/" + state + "/common/processed/videovector" + now.split(" ")(0)
    saveProcessedData(df_video_vector, videoProfilePath)

  }
  def getVideoVector(sparkSession: SparkSession, now: String, state: String)={
    val videoVectorPath = hdfsPath + "data/" + state + "/common/processed/videovector" + now.split(" ")(0)
    getData(sparkSession, videoVectorPath)
  }
  def savePlayList(now:String,df_play_list:DataFrame,state:String):Unit={
    val playListPath = hdfsPath + "data/" + state + "/common/processed/playlist" + now.split(" ")(0)
    saveProcessedData(df_play_list, playListPath)
  }
  def getPlayList(sparkSession: SparkSession, now: String, state: String)={
    val playListPath = hdfsPath + "data/" + state + "/common/processed/playlist" + now.split(" ")(0)
    getData(sparkSession, playListPath)
  }

  def saveOrderList(now:String,df_order_list:DataFrame,state:String):Unit={
    val orderListPath = hdfsPath + "data/" + state + "/common/processed/orderlist" + now.split(" ")(0)
    saveProcessedData(df_order_list, orderListPath)
  }
  def getOrderList(sparkSession: SparkSession, now: String, state: String)={
    val orderListPath = hdfsPath + "data/" + state + "/common/processed/orderlist" + now.split(" ")(0)
    getData(sparkSession,orderListPath)
  }
  def saveSinglePointTrainUsers(timeWindowStart: String,timeWindowEnd:String,df_all_train_users: DataFrame):Unit= {

    val trainSetUsersPath = hdfsPath + "data/train/singlepoint/userdivisiontraindata" + timeWindowStart.split(" ")(0)+"-"+timeWindowEnd.split(" ")(0)
    saveProcessedData(df_all_train_users, trainSetUsersPath)
  }
  def saveSinglePointTrainSamples(timeWindowStart: String,timeWindowEnd:String,df_all_train_users: DataFrame):Unit={
    val trainSetSamplesPath = hdfsPath + "data/train/singlepoint/ranktraindata" + timeWindowStart.split(" ")(0)+"-"+timeWindowEnd.split(" ")(0)
    saveProcessedData(df_all_train_users, trainSetSamplesPath)
  }
  def saveSinglePointPredictUsers(timeWindowStart: String,timeWindowEnd:String,df_all_predict_users: DataFrame):Unit= {

    val predictSetUsersPath = hdfsPath + "data/predict/singlepoint/userdivisionpredictdata" + timeWindowStart.split(" ")(0)+"-"+timeWindowEnd.split(" ")(0)
    saveProcessedData(df_all_predict_users, predictSetUsersPath)
  }
  def getUserDivisionResult(sparkSession: SparkSession, timeWindowStart: String,timeWindowEnd:String)={
    val userDivisionResult=hdfsPath + "data/predict/singlepoint/userdivisionresult"+timeWindowStart.split(" ")(0)+"-"+timeWindowEnd.split(" ")(0)

    getData(sparkSession,userDivisionResult)
  }
  def saveSinglePointPredictSamples(timeWindowStart: String,timeWindowEnd:String,df_all_predict_samples: DataFrame):Unit={
    val predictSetSamplesPath = hdfsPath + "data/predict/singlepoint/rankpredictdata" + timeWindowStart.split(" ")(0)+"-"+timeWindowEnd.split(" ")(0)
    saveProcessedData(df_all_predict_samples, predictSetSamplesPath)
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
