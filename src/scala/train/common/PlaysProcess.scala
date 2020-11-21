package train.common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.Utils.{printDf, udfAddSuffix}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._
import mam.Utils._

object PlaysProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var halfYearAgo: String = _
  val time_max_limit = 43200
  val time_min_limit = 30

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get sample users' play data.
    val df_raw_play = getRawPlayByDateRangeSmpleUsers(spark, halfYearAgo, partitiondate, license)

    printDf("df_raw_play", df_raw_play)

    // 2 - process of play data.
    val df_play = playProcess(df_raw_play)

    printDf("df_play", df_play)

    // 3 - save data to hive.
    // 可能需要改动 - 2020-11-11
    saveProcessedPlay(spark, df_play, partitiondate, license)
  }


  /**
    * Process of play data.
    *
    * @param df_raw_play
    * @return
    */
  def playProcess(df_raw_play: DataFrame) = {

    val df_play_processed = df_raw_play
      .na.drop()
      .withColumn(Dic.colIsOnlyNumberUserId, udfIsOnlyNumber(col(Dic.colUserId)))
      .withColumn(Dic.colIsOnlyNumberVideoId, udfIsOnlyNumber(col(Dic.colVideoId)))
      .withColumn(Dic.colIsOnlyNumberBroadcastTime, udfIsOnlyNumber(col(Dic.colBroadcastTime)))
      .withColumn(Dic.colIsFormattedTimePlayEndTime, udfIsFormattedTime(col(Dic.colPlayEndTime)))
      .filter(
        col(Dic.colIsOnlyNumberUserId).===(1)
          && col(Dic.colIsOnlyNumberVideoId).===(1)
          && col(Dic.colIsOnlyNumberBroadcastTime).===(1)
          && col(Dic.colIsFormattedTimePlayEndTime).===(1))
      .withColumn(Dic.colPlayEndTimeTmp, substring(col(Dic.colPlayEndTime), 0, 10))
      .drop(Dic.colPlayEndTime)
      .groupBy(col(Dic.colUserId), col(Dic.colVideoId), col(Dic.colPlayEndTimeTmp))
      .agg(
        sum(col(Dic.colBroadcastTime)) as Dic.colBroadcastTime)
      .filter(col(Dic.colBroadcastTime) < time_max_limit && col(Dic.colBroadcastTime) > time_min_limit)
      .orderBy(col(Dic.colUserId), col(Dic.colPlayEndTimeTmp))
      .withColumn(Dic.colPlayEndTime, udfAddSuffix(col(Dic.colPlayEndTimeTmp)))
      .drop(Dic.colPlayEndTimeTmp)

    df_play_processed
  }

}



