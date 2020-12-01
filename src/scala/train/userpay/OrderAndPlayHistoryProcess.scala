package train.userpay

/**
 * @author wx
 * @describe order中的订单历史，生成过去一个月的消费历史：包含支付成功和未支付成功
 */

import mam.Dic
import mam.Utils.{calDate, getData, printDf, saveProcessedData, udfGetAllHistoryVector, udfGetDays, udfGetErrorMoneySign, udfGetTopNHistory, udfLpad, udfUniformTimeValidity}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, count, desc, lit, row_number, sort_array}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrderAndPlayHistoryProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrderAndPlayHistoryProcessTrain")
      //.master("local[6]")
      .getOrCreate()

    val now = args(0) + " " + args(1)
    println(now)

    //val hdfsPath = ""
    val hdfsPath = "hdfs:///pay_predict/"
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp" //HDFS路径
    val mediasVideoVectorPath = hdfsPath + "data/train/common/processed/userpay/mediasVector"
    val historyPath = hdfsPath + "data/train/common/processed/userpay/history/"
    val trainSetUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)

    /**
     * Get Files
     */
    val df_orders = getData(spark, orderProcessedPath)
    val df_plays = getData(spark, playsProcessedPath)
    val df_medias = getData(spark, mediasProcessedPath)
    val df_trainUser = getData(spark, trainSetUsersPath)
    val df_trainId = df_trainUser.select(Dic.colUserId)

    /**
     * Get play history during train time to express package as package vector
     */
    val df_playTimesTrain = getVideoPlaysTimes(df_trainId, df_plays, now, 7, df_medias)
    printDf("df_playsHistoryTrainIn2Pack", df_playTimesTrain)

    saveProcessedData(df_playTimesTrain, historyPath + "videoInPackagePlayTimes")

    /**
     * Get user's order history in past three months (train time and predict time)
     * include payed and clicked history
     */

    val df_orderHistoryInPast = getOrderHistoryList(df_trainId, df_orders, now, -90)
    printDf("Users' orders history in past three months", df_orderHistoryInPast)

    saveProcessedData(df_orderHistoryInPast, historyPath + "orderHistory" + args(0))
    println("Order history process done! ")


    /**
     * Play history
     * 对于每个用户生成播放历史，14天内的播放历史，最多取n条
     */
    val timeWindowPlay = 14 //过去14天的
    val topNPlay = 50
    val df_playsVideo = getPlaySeqList(df_trainId, df_plays, Dic.colVideoId, now, -timeWindowPlay, topNPlay)
    printDf("df_playVideo", df_playsVideo)

    saveProcessedData(df_playsVideo, historyPath + "playHistory" + args(0))



    // play time, not use yet
    //    val df_playsTime = getPlaySeqList(df_trainId, df_plays, Dic.colTimeSum, now, -timeWindowPlay, topNPlay)
    //    val df_playsHistory = df_playsVideo.join(df_playsTime, Dic.colUserId)

    //    printDf("plays_list", df_playsHistory)
    //    saveProcessedData(df_playsHistory, savePath + "playHistory" + args(0))

    println("Play history process done!!")


  }


  def getOrderHistoryList(df_trainId: DataFrame, df_orders: DataFrame, now: String, timeLength: Int) = {

    /**
     * Select order history during now and now-timeLength
     * Create a new column called CreationTimeGap
     */
    val df_orderPart = df_orders.join(df_trainId, Seq(Dic.colUserId), "inner")
      .filter(col(Dic.colCreationTime) < now and col(Dic.colCreationTime) >= calDate(now, timeLength))
      .withColumn("now", lit(now))
      .withColumn(Dic.colCreationTimeGap, udfGetDays(col(Dic.colCreationTime), col("now")))
      //.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))
      .withColumn(Dic.colTimeValidity, udfUniformTimeValidity(col(Dic.colTimeValidity), col(Dic.colResourceType)))
      .select(Dic.colUserId, Dic.colMoney, Dic.colResourceType, Dic.colCreationTimeGap, Dic.colTimeValidity, Dic.colOrderStatus, Dic.colCreationTime)
      .join(df_trainId, Seq(Dic.colUserId), "inner")

    printDf("df_orderPart", df_orderPart)

    /**
     * This is to make sure the order history is in order after calculate in cluster
     */
    import org.apache.spark.sql.expressions.Window
    val win = Window.partitionBy(Dic.colUserId).orderBy(desc(Dic.colCreationTime))

    val rowCount = df_orderPart.count().toString.length
    val df_orderConcatCols = df_orderPart.withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"),
        concat_ws(",", col(Dic.colMoney), col(Dic.colResourceType), col(Dic.colCreationTimeGap), col(Dic.colTimeValidity), col(Dic.colOrderStatus)).as(Dic.colOrderHistory)))


    val df_orderHistoryUnionSameUser = df_orderConcatCols.groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn("tmp_column_list", udfGetAllHistoryVector(col("tmp_column_1")))
      .select(Dic.colUserId, "tmp_column_list")
      .withColumnRenamed("tmp_column_list", Dic.colOrderHistory)


    df_orderHistoryUnionSameUser

  }

  /**
   * We need to do the same for predict!!!!!!!!!!!!!!!!!!
   */
  def getVideoPlaysTimes(df_UserId: DataFrame, df_plays: DataFrame, train_time: String, timeLength: Int, df_medias: DataFrame) = {

    // train users' play history in train time
    val df_trainUserPlay = df_plays.filter(col(Dic.colPlayStartTime).<(train_time) and col(Dic.colPlayStartTime) >= calDate(train_time, days = -timeLength))
      .join(df_UserId, Seq(Dic.colUserId), "inner")
    //video in package that need to predict
    val df_videoInPredictPack = df_medias.filter(col(Dic.colPackageId) === 100201 or col(Dic.colPackageId) === 100202)
    val df_trainPlayHistory = df_trainUserPlay.join(df_videoInPredictPack, Seq(Dic.colVideoId))


    // Get video played times by users
    val df_videoPlayTimes = df_trainPlayHistory.groupBy(Dic.colVideoId).agg(count(Dic.colPlayStartTime).as(Dic.colPlayTimes))
    df_videoPlayTimes
  }

  def getPlaySeqList(df_trainId: DataFrame, df_play: DataFrame, colName: String, now: String, timeWindowPlay: Int, topNPlay: Int) = {
    /**
     * @describe 按照userid和播放起始时间逆向排序 选取 now - timewindow 到 now的播放历史和播放时长
     * @author wx
     * @param [plays]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset< org.apache.spark.sql.Row > }
     * */

    var df_playList = df_play.join(df_trainId, Seq(Dic.colUserId), "inner")
      .filter(col(Dic.colPlayStartTime).<(now) && col(Dic.colPlayStartTime) >= calDate(now, days = timeWindowPlay))

    //获取数字位数
    val rowCount = df_playList.count().toString.length
    println("df count number length", rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime).desc)

    /**
     * This part is to ensure the sequence has correct order in the spark cluster
     * The order is desc(play_start_time)
     */
    df_playList = df_playList.withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(colName)))


    df_playList = df_playList.groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(colName + "_list", udfGetTopNHistory(col("tmp_column_1"), lit(topNPlay)))
      .select(Dic.colUserId, colName + "_list")


    df_playList
  }

}