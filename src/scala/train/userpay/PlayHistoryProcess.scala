package train.userpay

/**
 * @author wx
 * @describe 用户播放历史
 */

import mam.Dic
import mam.Utils.{calDate, getData, printDf, saveProcessedData, udfGetTopNHistory, udfLpad, udfSplitArrayStrColumn}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, row_number, sort_array}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PlayHistoryProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("PlayHistory")
      .master("local[6]")
      .getOrCreate()

    val hdfsPath = ""
    //val hdfsPath = "hdfs:///pay_predict/"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasVideoVectorPath = hdfsPath + "data/train/common/processed/userpay/mediasVector"
    val savePath = hdfsPath + "data/train/common/processed/userpay/history/"


    val now = args(0) + " " + args(1)
    println(now)

    val timeWindow = 14 //过去14天的

    //play数据获取
    var df_plays = getData(spark, playsProcessedPath)
    printDf("plays", df_plays)

    val df_mediasVector = getData(spark, mediasVideoVectorPath)
    printDf("Medias vector", df_mediasVector)

    /**
     * 对于每个用户生成播放历史，14天内的播放历史，取n条，截取和补0
     */
    val topN = 50
    var df_playsVideo = getBehaviorSeqVector(df_plays, Dic.colVideoId, now, -timeWindow, topN)
    printDf("df_playVideo", df_playsVideo)


    /**
     * Get medias vector to map video ids in play history
     */


    val df_playsTime = getBehaviorSeqVector(df_plays, Dic.colTimeSum, now, -timeWindow, 50)

    val df_playsHistory = df_playsVideo.join(df_playsTime, Dic.colUserId)

    printDf("plays_list", df_playsHistory)


    saveProcessedData(df_playsHistory, savePath + "playHistory" + args(0))

    println("Play history process done!!")
  }


  def getBehaviorSeqVector(playDf: DataFrame, colName: String, now: String, timeWindow: Int, topN: Int) = {
    /**
     * @describe 按照userid和播放起始时间逆向排序 选取 now - timewindow 到 now的播放历史和播放时长
     * @author wx
     * @param [plays]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset< org.apache.spark.sql.Row > }
     * */

    var playList = playDf.filter(col(Dic.colPlayStartTime).<(now) && col(Dic.colPlayStartTime) >= calDate(now, days = timeWindow))

    //获取数字位数
    val rowCount = playList.count().toString.length
    println(rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime).desc)

    /**
     * This part is to ensure the sequence has correct order in the spark cluster
     * The order is desc(play_start_time)
     */
    playList = playList.withColumn("index", row_number().over(win))
      .withColumn("0", lit("0")) //要pad的字符
      .withColumn("tmp_rank", udfLpad(col("index"), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(colName)))

    printDf("row_numbers", playList)


    playList = playList.groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(colName + "_list", udfGetTopNHistory(col("tmp_column_1"), lit(topN)))
      .select(Dic.colUserId, colName + "_list")

    //    printDf("play history video id list", playList)

    playList
  }


}
