package predict.common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.Utils.udfAddSuffix
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import mam.Utils.printDf
import org.apache.spark.sql.functions._

object PlaysProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var today: String = _
  var yesterday: String = _
  var halfYearAgo: String = _
  var oneYearAgo: String = _
  val time_limit = 43200

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
    val df_raw_play = getPlay(spark)

    printDf("df_raw_play", df_raw_play)

    // 2 - process of play data.
    val df_play = playProcess(df_raw_play)

    printDf("df_play", df_play)

    // 3 - save data to hive.
    savePlay(spark, df_play)
  }


  /**
    * Get sample users' play data.
    *
    * @param spark
    */
  def getPlay(spark: SparkSession) = {

    println(today)
    println(halfYearAgo)
    println(license)

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    subscriber_id as subscriberid,
         |    time as play_end_time,
         |    item_id as video_id,
         |    sum_duration as broadcast_time
         |FROM
         |    vodrs.t_unified_user_behavior_distinct_day
         |WHERE
         |    partitiondate<='$today' and partitiondate>='$halfYearAgo' and product='vod' and stage='$license'
         |    and action='play' and event='play_end'
      """.stripMargin

    val df_user_play = spark.sql(user_play_sql)

    printDf("df_user_play", df_user_play)

    // 2 - sample users
    // 上线时此处要修改，增加 用户筛选的逻辑 & 此处的 hive表 需要变更 - Konverse -2020-10-29
    val sample_users_sql =
    """
      |SELECT
      |     subscriberid,rank
      |FROM
      |     vodrs.t_vod_user_sample_sdu_v1
      |WHERE
      |     partitiondate='20201029' and license='wasu' and shunt_subid<=6 and shunt_subid>=1
    """.stripMargin

    val df_sample_user = spark.sql(sample_users_sql)

    printDf("df_sample_user", df_sample_user)

    // 3 - join
    // 此处subid的逻辑上线时需要修改，使用真实subid - Konverse - 2020-10-29
    val df_raw_play = df_user_play.join(df_sample_user, Seq(Dic.colSubscriberid))
      .select(
        col(Dic.colRank).as(Dic.colUserId).cast(StringType),
        col(Dic.colPlayEndTime).cast(StringType),
        col(Dic.colVideoId).cast(StringType),
        col(Dic.colBroadcastTime).cast(DoubleType))

    df_raw_play
  }

  /**
    * Process of play data.
    * @param df_raw_play
    * @return
    */
  def playProcess(df_raw_play: DataFrame) = {

    val df_play = df_raw_play
      .withColumn(Dic.colPlayEndTime, substring(col(Dic.colPlayEndTime), 0, 10))
      .groupBy(
        col(Dic.colUserId),
        col(Dic.colVideoId),
        col(Dic.colPlayEndTime))
      .agg(
        sum(col(Dic.colBroadcastTime)).as(Dic.colBroadcastTime))
      .where(col(Dic.colBroadcastTime) < time_limit)
      .withColumn(Dic.colPlayEndTime, udfAddSuffix(col(Dic.colPlayEndTime)))

    df_play
  }

  /**
    * Save play data.
    * @param spark
    * @param df_order
    */
  def savePlay(spark: SparkSession, df_order: DataFrame) = {

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
    df_order.createOrReplaceTempView(tempTable)
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



