package train.common

import mam.Dic
import mam.GetSaveData.{getRawPlays, saveProcessedData}
import mam.Utils.{getData, printDf, sysParamSetting, udfLongToDateTime}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author wx
 * @param
 * @return
 * @describe 对 play数据的重新处理 划分 session 并对 session内的相同 video 时间间隔不超过30min的进行合并
 */

object PlaysProcessBySplitSession {

  val timeMaxLimit = 43200
  val timeMinLimit = 30
  val timeGapMergeForSameVideo = 1800 //相同视频播放间隔半小时内合并

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      //.master("local[4]")
      .appName("PlaysProcessBySplitSession")
      //      .enableHiveSupport()
      .getOrCreate()


    val hdfsPath = "hdfs:///pay_predict/"
//    val hdfsPath = ""

    val df_play_raw = getRawPlays(spark)
    printDf("输入 df_play_raw", df_play_raw)

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val playProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"

    val df_medias_processed = getData(spark, mediasProcessedPath)
    printDf("输入 df_medias_processed", df_medias_processed)

    val df_plays_processed = playsProcessBySpiltSession(df_play_raw, df_medias_processed)
//    saveProcessedData(df_plays_processed, playProcessedPath)

    printDf("输出 playProcessed", df_plays_processed)

    println("播放数据处理完成！")
  }

  def playsProcessBySpiltSession(df_play_raw: DataFrame, df_medias_processed: DataFrame): DataFrame = {
    /**
     * 转换数据类型
     */
    var df_play = df_play_raw.select(
      when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
      when(col(Dic.colPlayEndTime) === "NULL", null).otherwise(col(Dic.colPlayEndTime)).as(Dic.colPlayEndTime),
      when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
      when(col(Dic.colBroadcastTime) === "NULL", null).otherwise(col(Dic.colBroadcastTime)).as(Dic.colBroadcastTime)
    ).na.drop("any")
      .filter(col(Dic.colBroadcastTime) > timeMinLimit and col(Dic.colBroadcastTime) < timeMaxLimit)

    /**
     * 删除不在medias中的播放数据
     */

    val df_video_id = df_medias_processed.select(Dic.colVideoId).distinct()

    val df_play_in_medias = df_play.join(df_video_id, Seq(Dic.colVideoId), "inner")
    printDf("play数据中video存在medias中的数据", df_play_in_medias)


    /**
     * 计算开始时间 start_time
     */

    //end_time转换成long类型的时间戳    long类型 10位 单位 秒   colBroadcastTime是Int类型的 需要转化
    val df_play_start_time = df_play_in_medias.withColumn(Dic.colConvertTime, unix_timestamp(col(Dic.colPlayEndTime)))
      //计算开始时间并转化成时间格式
      .withColumn(Dic.colPlayStartTime, udfLongToDateTime(col(Dic.colConvertTime) - col(Dic.colBroadcastTime).cast("Long")))
      .drop(Dic.colConvertTime)

    /**
     * 根据用户id和 video id划分部分，然后每部分按照start_time进行排序 上移获得 start_time_Lead_play 和 start_time_Lead_same_video
     * 并 选取start_time_Lead_play和start_time_Lead_same_play 在 end_time之后的数据
     */

    //获得同一用户下一条 same video play数据的start_time
    val win1 = Window.partitionBy(Dic.colUserId, Dic.colVideoId).orderBy(Dic.colPlayStartTime)

    val df_play_gap = df_play_start_time
      //同一个用户下一个相同视频的开始时间
      .withColumn(Dic.colStartTimeLeadSameVideo, lead(Dic.colPlayStartTime, 1).over(win1)) //下一个start_time
      .withColumn(Dic.colTimeGapLeadSameVideo,
        ((unix_timestamp(col(Dic.colStartTimeLeadSameVideo))) - unix_timestamp(col(Dic.colPlayEndTime))))
      .withColumn(Dic.colTimeGap30minSign,
        when(col(Dic.colTimeGapLeadSameVideo) < timeGapMergeForSameVideo, 0) //相同视频播放时间差30min之内
          .otherwise(1)) //0和1不能反
      .withColumn(Dic.colTimeGap30minSignLag, lag(Dic.colTimeGap30minSign, 1).over(win1))
      //划分session
      .withColumn(Dic.colSessionSign, sum(Dic.colTimeGap30minSignLag).over(win1))
      //填充null 并选取 StartTimeLeadSameVideo 在 end_time之后的
      .na.fill(Map((Dic.colTimeGapLeadSameVideo, 0), (Dic.colSessionSign, 0))) //填充移动后产生的空值
      .filter(col(Dic.colTimeGapLeadSameVideo) >= 0) //筛选正确时间间隔的数据

    printDf("df_play_gap", df_play_gap)

    /**
     * 合并session内相同video时间间隔在30min之内的播放时长
     */

    val df_play_sum_time = df_play_gap
      .groupBy(
        Dic.colUserId,
        Dic.colVideoId,
        Dic.colSessionSign
      ).agg(
      sum(col(Dic.colBroadcastTime))
    )
      .withColumnRenamed("sum(broadcast_time)", Dic.colTimeSum)

    printDf("df_play_sum_time", df_play_sum_time)

    val df_play_session = df_play_gap
      .join(df_play_sum_time, Seq(Dic.colUserId, Dic.colVideoId, Dic.colSessionSign), "inner")
      .select(
        Dic.colUserId,
        Dic.colVideoId,
        Dic.colPlayStartTime,
        Dic.colTimeSum,
        Dic.colTimeGapLeadSameVideo,
        Dic.colSessionSign
      )

    printDf("df_play_session", df_play_session)
    /**
     * 同一个session内相同video只保留第一条数据
     */

    val win2 = Window.partitionBy(
      Dic.colUserId,
      Dic.colVideoId,
      Dic.colSessionSign,
      Dic.colTimeSum
    ).orderBy(Dic.colPlayStartTime)

    val df_play_processed = df_play_session
      .withColumn(Dic.colKeepSign, count(Dic.colSessionSign).over(win2))
      .filter(col(Dic.colKeepSign) === 1) //keep_sign为1的保留 其他全部去掉
      .drop(
        Dic.colKeepSign,
        Dic.colTimeGapLeadSameVideo,
        Dic.colSessionSign
      )
    printDf("df_play_processed", df_play_processed)

    df_play_processed
  }

}