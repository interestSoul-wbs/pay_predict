package train.userpay

/**
 * @author wx
 * @describe Train users' order history,  play history, and map video in play history to vector
 */


import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, mapIdToMediasVector, printDf, sysParamSetting, udfGetAllHistory, udfGetDays, udfGetErrorMoneySign, udfGetTopNHistory, udfLog, udfLpad, udfUniformTimeValidity}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, desc, lit, mean, row_number, sort_array, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrderAndPlayVectorProcess {

  val pastDaysForPlayHistory = 14
  val pastDaysForOrderHistory = 90
  val pastDaysPlaysForPackage = 7
  val topNPlay = 50

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrderAndPlayVectorProcessTrain")
      //.master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()

    val trainTime = args(0) + " " + args(1)
    println(trainTime)

    //val hdfsPath = ""
    val hdfsPath = "hdfs:///pay_predict/"
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val historyPath = hdfsPath + "data/train/common/processed/userpay/history/"

    val mediasVideoVectorPath = hdfsPath + "data/train/common/processed/userpay/mediasVector"
    val videoVectorSavePath = hdfsPath + "data/train/common/processed/userpay/videoVector"
    val playHistoryVectorSavePath = hdfsPath + "data/train/common/processed/userpay/history/playHistoryVector" + args(0)

    val trainUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)

    /**
     * Get Files
     */
    val df_orders = getData(spark, orderProcessedPath)
    printDf("df_orders", df_orders)
    val df_plays = getData(spark, playsProcessedPath)
    printDf("df_plays", df_plays)
    val df_medias = getData(spark, mediasProcessedPath)
    printDf("df_medias", df_medias)
    val df_train_user = getData(spark, trainUsersPath)
    printDf("df_train_user", df_train_user)
    val df_train_Id = df_train_user.select(Dic.colUserId)


    /**
     * Get train user's order history in past three months (train time and predict time)
     * include payed and clicked history
     */

    val df_order_history = getOrderHistoryList(df_train_Id, df_orders, trainTime, pastDaysForOrderHistory)
    printDf("df_order_history", df_order_history)

    saveProcessedData(df_order_history, historyPath + "orderHistory" + args(0))
    println("Order history process done! ")


    /**
     * Get train user's play history
     * 对于每个用户生成播放历史，14天内的播放历史，最多取n条
     */

    val df_play_history = getPlaySeqList(df_train_Id, df_plays, Dic.colVideoId, trainTime, pastDaysForPlayHistory, topNPlay)
    printDf("df_play_history", df_play_history)

    // 因为映射后向量存不下，因此把这个数据存储到HDFS上，然后用python运行的，所以如果后面的映射向量可以存储就不要存了
    //    saveProcessedData(df_play_history, historyPath + "playHistory" + args(0))

    /**
     * Map Video Id List To Vector !
     */

    // Get vector of medias' video
    val df_medias_Part = getData(spark, mediasVideoVectorPath)
    val df_video_vector = mediasVectorProcess(df_medias_Part, trainTime)
    printDf("df_video_vector", df_video_vector)

    // 因为映射后向量存不下，因此把这个数据存储到HDFS上，然后用python运行的，所以如果后面的映射向量可以存储就不要存了
    saveProcessedData(df_video_vector, videoVectorSavePath)

    val df_user_play_vector = mapVideoVector(df_play_history, df_video_vector, topNPlay)

    printDf("df_user_play_vector", df_user_play_vector)

    saveProcessedData(df_user_play_vector, playHistoryVectorSavePath)
    println("Play history vector process done!!")


    /**
     * Get all User;s play history play history during train time to express package as package vector
     */
    val df_video_played_times = getVideoPlaysTimes(df_plays, trainTime, pastDaysPlaysForPackage, df_medias)
    printDf("df_video_played_times", df_video_played_times)

    /**
     * Use videos' vector in package to express package
     */
    val df_video_times_vector = df_video_vector.join(df_video_played_times, Seq(Dic.colVideoId), "inner") //video vector is played video
      .withColumn("log_count", when(col(Dic.colPlayTimes) > 0, udfLog(col(Dic.colPlayTimes))).otherwise(0))
      .select(Dic.colVideoId, "vector", "log_count")


    printDf("df_video_times_vector", df_video_times_vector)

    //    Use python to select "weighted_vector" to be a Matrix then merge with every user
    saveProcessedData(df_video_times_vector, historyPath + "packageExpByPlayedVideo" + args(0))

    println("packageExpByVideo dataframe save done!")


  }


  def getOrderHistoryList(df_train_id: DataFrame, df_orders: DataFrame, now: String, timeLength: Int) = {

    /**
     * Select order history during now and now-timeLength
     * Create a new column called CreationTimeGap
     */
    val df_order_part = df_orders.join(df_train_id, Seq(Dic.colUserId), "inner")
      .filter(col(Dic.colCreationTime) < now and col(Dic.colCreationTime) >= calDate(now, -timeLength))
      .withColumn("now", lit(now))
      .withColumn(Dic.colCreationTimeGap, udfGetDays(col(Dic.colCreationTime), col("now")))
      //.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))
      .withColumn(Dic.colTimeValidity, udfUniformTimeValidity(col(Dic.colTimeValidity), col(Dic.colResourceType)))
      .select(Dic.colUserId, Dic.colMoney, Dic.colResourceType, Dic.colCreationTimeGap, Dic.colTimeValidity, Dic.colOrderStatus, Dic.colCreationTime)


    printDf("df_orderPart", df_order_part)

    /**
     * This is to make sure the order history is in order after calculate in cluster
     */
    import org.apache.spark.sql.expressions.Window
    val win = Window.partitionBy(Dic.colUserId).orderBy(desc(Dic.colCreationTime))

    val rowCount = df_order_part.count().toString.length
    val df_order_concat_columns = df_order_part.withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"),
        concat_ws(",", col(Dic.colMoney), col(Dic.colResourceType), col(Dic.colCreationTimeGap), col(Dic.colTimeValidity), col(Dic.colOrderStatus)).as(Dic.colOrderHistory)))


    val df_order_history = df_order_concat_columns.groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn("tmp_column_list", udfGetAllHistory(col("tmp_column_1")))
      .select(Dic.colUserId, "tmp_column_list")
      .withColumnRenamed("tmp_column_list", Dic.colOrderHistory)


    df_order_history

  }


  def getVideoPlaysTimes(df_plays: DataFrame, train_time: String, timeLength: Int, df_medias: DataFrame) = {
    /**
     * 获得套餐内视频的被播放次数，用这部分视频及其播放次数加权后的向量表示套餐信息
     */

    val df_train_play = df_plays.filter(col(Dic.colPlayStartTime).<(train_time) and col(Dic.colPlayStartTime) >= calDate(train_time, days = -timeLength))

    //video in package that need to predict
    val df_video_in_package = df_medias.filter(col(Dic.colPackageId) === 100201 or col(Dic.colPackageId) === 100202)
    val df_play_history = df_train_play.join(df_video_in_package, Seq(Dic.colVideoId), "inner")


    // Get video played times by users
    df_play_history.groupBy(Dic.colVideoId).agg(count(Dic.colPlayStartTime).as(Dic.colPlayTimes))
  }


  def getPlaySeqList(df_train_id: DataFrame, df_play: DataFrame, colName: String, now: String, timeWindowPlay: Int, topNPlay: Int) = {
    /**
     * @describe 按照userid和播放起始时间逆向排序 选取 now - timewindow 到 now的播放历史和播放时长
     * @author wx
     * @param [plays]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset< org.apache.spark.sql.Row > }
     * */

    var df_train_play = df_play.join(df_train_id, Seq(Dic.colUserId), "inner")
      .filter(col(Dic.colPlayStartTime).<(now) && col(Dic.colPlayStartTime) >= calDate(now, days = -timeWindowPlay))

    //获取数字位数
    val rowCount = df_train_play.count().toString.length
    println("df count number length", rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime).desc)

    /**
     * This part is to ensure the sequence has correct order after spark cluster
     * The order is desc(play_start_time)
     */
    df_train_play.withColumn("index", row_number().over(win))
      //为播放的视频id进行排序
      .withColumn("0", lit("0"))
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(colName)))
      //将list中的video按照播放次序排列
      .groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(colName + "_list", udfGetTopNHistory(col("tmp_column_1"), lit(topNPlay)))
      .select(Dic.colUserId, colName + "_list")

  }

  def mediasVectorProcess(df_medias_vector_part: DataFrame, trainTime: String) = {
    /**
     * @description: Add storage time gap to medias info then assemble to video vector
     * @param: df_medias_vector_part : medias dataframe
     * @param: trainTime
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/30
     */

    val df_medias_vector = df_medias_vector_part.withColumn("trainTime", lit(trainTime))
      .withColumn(Dic.colStorageTimeGap, udfGetDays(col(Dic.colStorageTime), col("trainTime")))
      .drop("trainTime", Dic.colStorageTime)

    // fill na colStorageTime with colLevelOne mean storage time
    val df_filled_gap = fillStorageGap(df_medias_vector)

    printDf("df_mediasVector", df_filled_gap)

    // Concat columns to be vector of the videos
    val mergeColumns = df_filled_gap.columns.filter(!_.contains(Dic.colVideoId)) //remove column videoId
    val assembler = new VectorAssembler()
      .setInputCols(mergeColumns)
      .setHandleInvalid("keep")
      .setOutputCol("vector")

    assembler.transform(df_filled_gap).select(Dic.colVideoId, "vector")

  }


  def fillStorageGap(df_medias: DataFrame): DataFrame = {
    /**
     * @describe 根据video的视频一级分类进行相关列空值的填充
     * @author wx
     * @param [mediasDf]
     * @param [spark]
     * @return {@link DataFrame }
     * */
    val df_mean = df_medias.groupBy(Dic.colVideoOneLevelClassification).agg(mean(col(Dic.colStorageTimeGap)))
      .withColumnRenamed("avg(" + Dic.colStorageTimeGap + ")", "mean_" + Dic.colStorageTimeGap)

    printDf("medias中一级分类的" + Dic.colStorageTimeGap + "平均值", df_mean)

    // video的colName全部video平均值
    val meanValue = df_medias
      .agg(mean(Dic.colStorageTimeGap))
      .collectAsList()
      .get(0)
      .get(0)

    println("mean " + Dic.colStorageTimeGap, meanValue)


    val df_meanFilled = df_medias.join(df_mean, Seq(Dic.colVideoOneLevelClassification), "inner")
      .withColumn(Dic.colStorageTimeGap, when(col(Dic.colStorageTimeGap).>=(0.0), col(Dic.colStorageTimeGap))
        .otherwise(col("mean_" + Dic.colStorageTimeGap)))
      .na.fill(Map((Dic.colStorageTimeGap, meanValue)))
      .drop("mean_" + Dic.colStorageTimeGap)
      .withColumn(Dic.colStorageTimeGap, udfLog(col(Dic.colStorageTimeGap)))
      .drop(Dic.colVideoOneLevelClassification)

    df_meanFilled
  }

  def mapVideoVector(df_play_history: DataFrame, df_video_vector: DataFrame) = {
    /**
     * @description: Map the video id list to vector
     * @param:df_playHistory : df_play_history Dataframe which has video id list
     * @param: df_video_vector  : Video Vector Dataframe, (columns video_id, vector)
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


    val df_playVector = df_play_history.withColumn("play_vector", mapIdToMediasVector(mediasMap)(col(Dic.colVideoId + "_list")))
      .drop(Dic.colVideoId + "_list")

    df_playVector

  }

}