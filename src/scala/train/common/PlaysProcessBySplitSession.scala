package train.common

import mam.Dic
import mam.Utils.{getData, printDf, saveProcessedData, udfAddSuffix, udfLongToDateTime}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * 对 play数据的重新处理 划分 session 并对 session内的相同 video 时间间隔不超过30min的进行合并
 */

object PlaysProcessBySplitSession {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .master("local[4]")
      .appName("PlaysProcessBySplitSession")
      .getOrCreate()


    //val hdfsPath = "hdfs:///pay_predict/"
    val hdfsPath = ""

    val playRawPath = hdfsPath + "data/train/common/raw/plays"
    val playProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val playRaw = getRawPlays(playRawPath, spark)
    printDf("输入 playRaw", playRaw)

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp" //HDFS路径
    val mediasProcessed = getData(spark, mediasProcessedPath)

    val playsProcessed = playsProcessBySpiltSession(playRaw, mediasProcessed)
    printDf("输出 playProcessed", playsProcessed)

    saveProcessedData(playsProcessed, playProcessedPath)
    println("播放数据处理完成！")
  }

  def getRawPlays(playRawPath: String, spark: SparkSession) = {
    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)
      )
    )
    val df = spark.read
      .option("delimiter", "\\t")
      .option("header", false)
      .schema(schema)
      .csv(playRawPath)
    df

  }

  def playsProcessBySpiltSession(playRaw: DataFrame, mediasProcessed: DataFrame): DataFrame = {
    /**
     * 转换数据类型
     */
    var play = playRaw.select(
      when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
      when(col(Dic.colPlayEndTime) === "NULL", null).otherwise(col(Dic.colPlayEndTime)).as(Dic.colPlayEndTime),
      when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
      when(col(Dic.colBroadcastTime) === "NULL", null).otherwise(col(Dic.colBroadcastTime)).as(Dic.colBroadcastTime)
    )

    play = play.dropDuplicates().na.drop("any")
    printDf("去除重复, 空数据之后", play)

    /**
     * 删除单条播放数据不足30s或超过12h的播放记录
     */

    play = play.filter(col(Dic.colBroadcastTime) > 30 and col(Dic.colBroadcastTime) < 43200)
    printDf("大于30s小于12h的播放数据", play)

    /**
     * 删除不在medias中的播放数据
     */

    val mediasVideo = mediasProcessed.select(Dic.colVideoId).distinct()

    play = play.join(mediasVideo, Seq(Dic.colVideoId), "inner")
    printDf("play数据中video存在medias中的数据", play)


    /**
     * 计算开始时间 start_time
     */

    //end_time转换成long类型的时间戳    long类型 10位 单位 秒   colBroadcastTime是Int类型的 需要转化
    play = play.withColumn(Dic.colConvertTime, unix_timestamp(col(Dic.colPlayEndTime)))
      //计算开始时间并转化成时间格式
      .withColumn(Dic.colPlayStartTime, udfLongToDateTime(col(Dic.colConvertTime) - col(Dic.colBroadcastTime).cast("Long")))
      .drop(Dic.colConvertTime)

    /**
     * 根据用户id和 video id划分部分，然后每部分按照start_time进行排序 上移获得 start_time_Lead_play 和 start_time_Lead_same_video
     * 并 选取start_time_Lead_play和start_time_Lead_same_play 在 end_time之后的数据
     */


    //获得同一用户下一条 same video play数据的start_time
    val win1 = Window.partitionBy(Dic.colUserId, Dic.colVideoId).orderBy(Dic.colPlayStartTime)
    //同一个用户下一个相同视频的开始时间
    play = play.withColumn(Dic.colStartTimeLeadSameVideo, lead(Dic.colPlayStartTime, 1).over(win1)) //下一个start_time
      .withColumn(Dic.colTimeGapLeadSameVideo, ((unix_timestamp(col(Dic.colStartTimeLeadSameVideo))) - unix_timestamp(col(Dic.colPlayEndTime))))
      .withColumn(Dic.colTimeGap30minSign, when(col(Dic.colTimeGapLeadSameVideo) < 1800, 0).otherwise(1)) //0和1不能反
      .withColumn(Dic.colTimeGap30minSignLag, lag(Dic.colTimeGap30minSign, 1).over(win1))
      //划分session
      .withColumn(Dic.colSessionSign, sum(Dic.colTimeGap30minSignLag).over(win1))
      //填充null 并选取 StartTimeLeadSameVideo 在 end_time之后的
      .na.fill(Map((Dic.colTimeGapLeadSameVideo, 0), (Dic.colSessionSign, 0))) //填充空值
      .filter(col(Dic.colTimeGapLeadSameVideo) >= 0) //筛选正确时间间隔的数据

    /**
     * 合并session内相同video时间间隔在30min之内的播放时长
     */

    val timeSum = play.groupBy(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign).agg(sum(col(Dic.colBroadcastTime)))
      .withColumnRenamed("sum(broadcast_time)", Dic.colTimeSum)

    play = play.join(timeSum, Seq(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign), "inner")
      .select(Dic.colUserId, Dic.colVideoId, Dic.colPlayStartTime, Dic.colTimeSum, Dic.colTimeGapLeadSameVideo, Dic.colSessionSign)

    /**
     * 同一个session内相同video只保留第一条数据
     */

    val win2 = Window.partitionBy(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign, Dic.colTimeSum).orderBy(Dic.colPlayStartTime)
    play = play.withColumn(Dic.colKeepSign, count(Dic.colSessionSign).over(win2)) //keep_sign为1的保留 其他全部去重
      .filter(col(Dic.colKeepSign) === 1)
      .drop(col(Dic.colKeepSign))
      .drop(col(Dic.colTimeGapLeadSameVideo))
      .drop(col(Dic.colSessionSign))

    play
  }

}