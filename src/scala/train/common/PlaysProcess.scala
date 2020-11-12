package train.common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.Utils.{printDf, udfAddSuffix}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._
object PlaysProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var today: String = _
  var yesterday: String = _
  var halfYearAgo: String = _
  var oneYearAgo: String = _
  val time_max_limit=43200
  val time_min_limit=30

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    today = date.toString(DateTimeFormat.forPattern("yyyyMMdd"))
    yesterday = (date - 1.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    oneYearAgo = (date - 365.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get sample users' play data.
    val df_raw_play = getRawPlayByDateRangeSmpleUsers(spark, halfYearAgo, today, license)

    printDf("df_raw_play", df_raw_play)

    // 2 - process of play data.
    val df_play = playProcess(df_raw_play)

    printDf("df_play", df_play)

    // 3 - save data to hive.
    // 可能需要改动 - 2020-11-11
    saveProcessedPlay(spark, df_play)
  }



  /**
    * Process of play data.
    * @param df_raw_play
    * @return
    */
  def playProcess(df_raw_play: DataFrame) = {

    val playProcessed=df_raw_play
      .withColumn(Dic.colPlayEndTime,substring(col(Dic.colPlayEndTime),0,10))
      .groupBy(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colPlayEndTime))
      .agg(
        sum(col(Dic.colBroadcastTime)) as Dic.colBroadcastTime)
      .filter(col(Dic.colBroadcastTime)<time_max_limit && col(Dic.colBroadcastTime)>time_min_limit )
      .orderBy(col(Dic.colUserId),col(Dic.colPlayEndTime))
      .withColumn(Dic.colPlayEndTime,udfAddSuffix(col(Dic.colPlayEndTime)))

    playProcessed
  }

  /**
    * Save play data.
    * @param spark
    * @param df_order
    */
  def saveProcessedPlay(spark: SparkSession, df_play: DataFrame) = {

    // 1 - If table not exist, creat.
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.t_sdu_user_play_history_paypredict(
        |         user_id string,
        |         video_id string,
        |         play_end_time string,
        |         broadcast_time double)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    // 2 - Save data.
    println("save data to hive........... \n" * 4)
    df_play.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_sdu_user_play_history_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}



