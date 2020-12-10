package predict.userpay

/**
 * @author wx
 * @describe order中的订单历史，生成过去一个月的消费历史：包含支付成功和未支付成功
 */

import breeze.linalg.DenseVector
import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, mapIdToMediasVector, printDf, sysParamSetting, udfGetAllOrderHistory, udfGetDays, udfGetTopNHistory, udfLog, udfLpad, udfUniformTimeValidity}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrderAndPlayVectorProcess {

  val pastDaysForPlayHistory = 14
  val pastDaysForOrderHistory = 90
  val pastDaysPlaysForPackage = 7
  val topNPlay = 50
  val videoVectorDimension = 37

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrderAndPlayVectorProcessPredict")
      .master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()

    val predictTime = args(0) + " " + args(1)
    println(predictTime)

    /**
     * Data Path
     */
    val hdfsPath = ""
    //val hdfsPath = "hdfs:///pay_predict/"
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val mediasVideoVectorPath = hdfsPath + "data/train/common/processed/userpay/mediasVector"

    val historyPath = hdfsPath + "data/predict/common/processed/userpay/history/"
    val videoVectorSavePath = hdfsPath + "data/predict/common/processed/userpay/videoVector"
    val playHistoryVectorSavePath = hdfsPath + "data/predict/common/processed/userpay/history/playHistoryVector" + args(0)

    val predictUsersPath = hdfsPath + "data/predict/userpay/predictUsers" + args(0)


    /**
     * Get Files
     */
    val df_orders = getData(spark, orderProcessedPath)
    printDf("输入 df_orders", df_orders)

    val df_plays = getData(spark, playsProcessedPath)
    printDf("输入 df_plays", df_plays)

    val df_medias = getData(spark, mediasProcessedPath)
    printDf("输入 df_medias", df_medias)

    val df_predict_user = getData(spark, predictUsersPath)
    printDf("输入 df_predict_ser", df_predict_user)

    val df_medias_vector_v1 = getData(spark, mediasVideoVectorPath)
    printDf("输入 df_medias_vector_v1", df_medias_vector_v1)


    val df_predict_id = df_predict_user.select(Dic.colUserId)


    /**
     * Get predict user's order history in past three months 
     * include payed and clicked history
     */

    val df_order_history = getOrderHistoryList(df_predict_id, df_orders, predictTime, pastDaysForOrderHistory)
    printDf("输出 df_order_history in past three months", df_order_history)

    //    saveProcessedData(df_order_history, historyPath + "orderHistory" + args(0))
    println("Order history process done! ")


    /**
     * Get predict user's play history
     * 对于每个用户生成播放历史，14天内的播放历史，最多取n条
     */
    val df_predict_play_history = getPlaySeqList(df_predict_id, df_plays, Dic.colVideoId, predictTime, pastDaysForPlayHistory, topNPlay)
    printDf("输出 df_predict_play_history", df_predict_play_history)

    // 因为映射后向量存不下，因此把这个数据存储到HDFS上，然后用python运行的，所以如果后面的映射向量可以存储就不要存了
    //    saveProcessedData(df_predict_play_history, historyPath + "playHistory" + args(0))

    /**
     * Map Video Id List To Vector !
     */
    // Get Video Vector
    val df_video_vector = mediasVectorProcess(df_medias_vector_v1, predictTime)
    printDf("输出 df_video_vector", df_video_vector)

    // 因为映射后向量存不下，因此把这个数据存储到HDFS上，然后用python运行的，所以如果后面的映射向量可以存储就不要存了
    //    saveProcessedData(df_video_vector, videoVectorSavePath)
    println("User's played video vector save done!")

    // 播放历史映射
    val df_predict_play_Vector = mapVideoVector(df_predict_play_history, df_video_vector, topNPlayHistory = 50)
    printDf("输出 df_predict_play_Vector", df_predict_play_Vector)
    //    saveProcessedData(df_predict_play_Vector, playHistoryVectorSavePath)
    println("Play history vector process done!!!")


    /**
     * Get all User's play history play history during train time to express package as package vector
     */
    val df_video_played_times = getVideoPlaysTimes(df_plays, predictTime, pastDaysPlaysForPackage, df_medias)
    val df_video_times_vector = getPackageVector(df_video_vector, df_video_played_times)

    printDf("输出 df_video_times_vector", df_video_times_vector)

    //    Use python to select "weighted_vector" to be a Matrix then merge with every user
    //    saveProcessedData(df_video_times_vector, historyPath + "packageExpByPlayedVideo" + args(0))

    println("packageExpByVideo dataframe save done!")


  }


  def getOrderHistoryList(df_predict_id: DataFrame, df_orders: DataFrame, now: String, timeLength: Int) = {

    /**
     * Select order history during now and now-timeLength
     * Create a new column called CreationTimeGap
     */
    val df_order_part = df_orders
      .join(df_predict_id, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colCreationTime) < now
          && col(Dic.colCreationTime) >= calDate(now, -timeLength)
      )
      .withColumn("now", lit(now))
      .withColumn(Dic.colCreationTimeGap, udfGetDays(col(Dic.colCreationTime), col("now")))
      //.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))
      .withColumn(Dic.colTimeValidity, udfUniformTimeValidity(col(Dic.colTimeValidity), col(Dic.colResourceType)))
      .select(
        Dic.colUserId,
        Dic.colMoney,
        Dic.colResourceType,
        Dic.colCreationTimeGap,
        Dic.colTimeValidity,
        Dic.colOrderStatus,
        Dic.colCreationTime
      )


    printDf("df_order_part", df_order_part)

    /**
     * This is to make sure the order history is in order after calculate in cluster
     */
    import org.apache.spark.sql.expressions.Window
    val win = Window.partitionBy(Dic.colUserId).orderBy(desc(Dic.colCreationTime))

    val rowCount = df_order_part.count().toString.length
    val df_order_concat_columns = df_order_part
      // 排序前准备
      .withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"),
        concat_ws(",",
          col(Dic.colMoney),
          col(Dic.colResourceType),
          col(Dic.colCreationTimeGap),
          col(Dic.colTimeValidity),
          col(Dic.colOrderStatus)
        ).as(Dic.colOrderHistory)))
      // 获得个人历史且按照顺序存储
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")
      ) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn("tmp_column_list", udfGetAllOrderHistory(col("tmp_column_1")))
      .select(Dic.colUserId, "tmp_column_list")
      .withColumnRenamed("tmp_column_list", Dic.colOrderHistory)


    df_order_concat_columns

  }


  def getVideoPlaysTimes(df_plays: DataFrame, predict_time: String, timeLength: Int, df_medias: DataFrame) = {

    // predict users' play history in predict time
    val df_predict_play = df_plays
      .filter(
        col(Dic.colPlayStartTime).<(predict_time)
          && col(Dic.colPlayStartTime) >= calDate(predict_time, days = -timeLength)
      )

    //video in package that need to predict
    val df_package_video = df_medias
      .filter(col(Dic.colPackageId) === 100201
        or col(Dic.colPackageId) === 100202)

    val df_package_play_history = df_predict_play.join(df_package_video, Seq(Dic.colVideoId), "inner")

    // Get video played times by users
    df_package_play_history
      .groupBy(Dic.colVideoId)
      .agg(
        count(Dic.colPlayStartTime).as(Dic.colPlayTimes)
      )
  }


  def getPlaySeqList(df_predict_id: DataFrame, df_play: DataFrame, colName: String, now: String, timeWindowPlay: Int, topNPlay: Int) = {
    /**
     * @describe 按照userid和播放起始时间逆向排序 选取 now - timewindow 到 now的播放历史和播放时长
     * @author wx
     * @param [plays]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset< org.apache.spark.sql.Row > }
     * */

    val df_play_list = df_play
      .join(df_predict_id, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colPlayStartTime).<(now)
          && col(Dic.colPlayStartTime) >= calDate(now, days = -timeWindowPlay)
      )

    //获取数字位数
    val rowCount = df_play_list.count().toString.length
    println("df count number length", rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime).desc)

    /**
     * This part is to ensure the sequence has correct order after spark cluster
     * The order is desc(play_start_time)
     */
    df_play_list
      .withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(colName)))
      .groupBy(col(Dic.colUserId))
      .agg(
        collect_list(col("tmp_column")).as("tmp_column")
      ) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(colName + "_list", udfGetTopNHistory(col("tmp_column_1"), lit(topNPlay)))
      .select(Dic.colUserId, colName + "_list")


  }

  def mediasVectorProcess(df_medias_vector_v1: DataFrame, predictTime: String) = {
    /**
     * @description: Add storage time gap to medias info then assemble to video vector
     * @param: df_medias_vector_v1 : medias dataframe
     * @param: predictTime
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/30
     */

    val df_medias_vector = df_medias_vector_v1
      .withColumn("predictTime", lit(predictTime))
      .withColumn(Dic.colStorageTimeGap, udfGetDays(col(Dic.colStorageTime), col("predictTime")))
      .drop("predictTime", Dic.colStorageTime)

    // fill na colStorageTime with colLevelOne mean storage time
    val df_filled_medias = fillStorageGap(df_medias_vector)
    printDf("df_medias_vector", df_filled_medias)

    // Concat columns to be vector of the videos
    val mergeColumns = df_filled_medias.columns.filter(!_.contains(Dic.colVideoId)) //remove column videoId
    val assembler = new VectorAssembler()
      .setInputCols(mergeColumns)
      .setHandleInvalid("keep")
      .setOutputCol("vector")

    assembler.transform(df_filled_medias).select(Dic.colVideoId, "vector")

  }


  def fillStorageGap(df_medias: DataFrame): DataFrame = {
    /**
     * @describe 根据video的视频一级分类进行相关列空值的填充
     * @author wx
     * @param [mediasDf]
     * @param [spark]
     * @return {@link DataFrame }
     * */
    val df_mean = df_medias
      .groupBy(Dic.colVideoOneLevelClassification)
      .agg(
        mean(col(Dic.colStorageTimeGap))
      )
      .withColumnRenamed("avg(" + Dic.colStorageTimeGap + ")", "mean_" + Dic.colStorageTimeGap)

    printDf("medias中一级分类的" + Dic.colStorageTimeGap + "平均值", df_mean)

    // video的colName全部video平均值
    val meanValue = df_medias
      .agg(
        mean(Dic.colStorageTimeGap)
      )
      .collectAsList()
      .get(0)
      .get(0)

    println("mean " + Dic.colStorageTimeGap, meanValue)


    val df_medias_mean = df_medias.join(df_mean, Seq(Dic.colVideoOneLevelClassification), "inner")
    printDf("df_medias_mean", df_medias_mean)


    df_medias_mean
      .withColumn(Dic.colStorageTimeGap, when(col(Dic.colStorageTimeGap).>=(0.0), col(Dic.colStorageTimeGap))
        .otherwise(col("mean_" + Dic.colStorageTimeGap)))
      .na.fill(Map((Dic.colStorageTimeGap, meanValue)))
      .drop("mean_" + Dic.colStorageTimeGap)
      .withColumn(Dic.colStorageTimeGap, udfLog(col(Dic.colStorageTimeGap)))
      .drop(Dic.colVideoOneLevelClassification)
  }

  def mapVideoVector(df_predict_play_history: DataFrame, df_video_vector: DataFrame, topNPlayHistory: Int) = {
    /**
     * @description: Map the video id list to vector
     * @param:df_playHistory : df_playHistory Dataframe which has video id list
     * @param: df_video_vector  : Video Vector Dataframe, (columns video_id, vector)
     * @param: topNPlayHistory : play history video's number
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/26
     */

    import scala.collection.mutable
    /**
     * Medias to Map( video_id -> vector)
     */
    val mediasMap = df_video_vector.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoId).toString -> row.getAs("vector").toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    println("Medias Map size", mediasMap.size)

    printDf("df_predict_playHistory", df_predict_play_history)

    val filledList = List.fill(videoVectorDimension)(0)
    val df_play_vector = df_predict_play_history
      .withColumn("play_vector", mapIdToMediasVector(mediasMap)(col(Dic.colVideoId + "_list"), lit(filledList)))
      .drop(Dic.colVideoId + "_list")

    df_play_vector

  }


  def getPackageVector(df_video_vector: DataFrame, df_video_played_times: DataFrame): DataFrame = {
    /**
     * @description: Use videos in package 100201 and 100202 to express package info
     * @param: df_video_vector
     * @param: df_video_played_times
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/12/4
     */

    df_video_vector
      .join(df_video_played_times, Seq(Dic.colVideoId), "inner") //video vector is played video
      .withColumn("log_count",
        when(col(Dic.colPlayTimes) > 0, udfLog(col(Dic.colPlayTimes)))
          .otherwise(0)
      )
      .select(Dic.colVideoId, "vector", "log_count")

  }


}
