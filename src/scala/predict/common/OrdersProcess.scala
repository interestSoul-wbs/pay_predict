package predict.common

/**
  * @Author wj
  * @Date 2020/09
  * @Version 1.0
  */

import com.github.nscala_time.time.Imports.DateTimeFormat
import mam.Dic
import mam.Utils.udfChangeDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import mam.Utils.printDf
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.functions._
import mam.GetSaveData._

object OrdersProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var today: String = _
  var yesterday: String = _
  var halfYearAgo: String = _
  var oneYearAgo: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    today = date.toString(DateTimeFormat.forPattern("yyyyMMdd"))
    yesterday = (date - 1.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    oneYearAgo = (date - 365.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get all raw order data.
    val df_raw_order = getRawOrderByDateRange(spark, halfYearAgo, today, license)

    // 2 -

    printDf("df_raw_order", df_raw_order)

    // 2 - process of order data.
    val df_order = multiOrderTimesProcess(df_raw_order)

    printDf("df_order", df_order)

    // 3 - save data to hive.
    saveOrder(spark, df_order)

    println("预测阶段订单数据处理完成！")
  }

  /**
    * Get smaple users' order data within a year.
    * @param spark
    * @return
    */
  def getRawOrder(spark: SparkSession) = {

    println(today)
    println(halfYearAgo)
    println(license)

    // 1 - 获取用户购买记录
    val user_order_ori_sql =
      s"""
         |select
         |    userid as subscriberid,fee,resourcetype,resourceid,createdtime,discountid,status,resourcename,starttime,endtime
         |from
         |    vodbasicdim.o_com_vod_all_order
         |where
         |    partitiondate<='$today' and partitiondate>='$halfYearAgo' and licence='$license'
      """.stripMargin

    val df_order_ori = spark.sql(user_order_ori_sql)

    printDf("df_order_ori", df_order_ori)

    // 2 - 订单信息
    val order_info_sql =
      s"""
         |SELECT
         |    id as discountid,desc as discountdesc
         |FROM
         |    vodbasicdim.o_vod_ws_discount_info_day
         |WHERE
         |    partitiondate='$today'
      """.stripMargin

    val df_order_info = spark.sql(order_info_sql)

    printDf("df_order_info", df_order_info)

    val df_user_order = df_order_ori.join(df_order_info, Seq(Dic.colDiscountid), "left")

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
    val df_raw_order = df_user_order.join(df_sample_user, Seq(Dic.colSubscriberid))
      .selectExpr("rank as subscriberid", "fee", "resourcetype", "resourceid", "resourcename", "createdtime",
        "discountdesc", "status", "starttime", "endtime")
      .select(
        col(Dic.colSubscriberid).as(Dic.colUserId),
        col(Dic.colFee).as(Dic.colMoney),
        col(Dic.colResourcetype).as(Dic.colResourceType),
        col(Dic.colResourceid).as(Dic.colResourceId),
        col(Dic.colResourcename).as(Dic.colResourceTitle),
        col(Dic.colCreatedtime).as(Dic.colCreationTime),
        col(Dic.colDiscountdesc).as(Dic.colDiscountDescription),
        col(Dic.colStatus).as(Dic.colOrderStatus),
        col(Dic.colStarttime).as(Dic.colOrderStartTime),
        col(Dic.colEndtime).as(Dic.colOrderEndTime))

    df_raw_order
  }

  /**
    * Process of order data.
    * @param df_raw_order
    * @return
    */
  def multiOrderTimesProcess(df_raw_order: DataFrame) = {

    val df_order = df_raw_order
      .select(
        col(Dic.colUserId).cast(StringType),
        col(Dic.colMoney).cast(DoubleType),
        col(Dic.colResourceType).cast(DoubleType),
        col(Dic.colResourceId).cast(StringType),
        col(Dic.colResourceTitle).cast(StringType),
        udfChangeDateFormat(col(Dic.colCreationTime)).cast(StringType).as(Dic.colCreationTime),
        col(Dic.colDiscountDescription).cast(StringType),
        col(Dic.colOrderStatus).cast(DoubleType),
        udfChangeDateFormat(col(Dic.colOrderStartTime)).cast(StringType).as(Dic.colOrderStartTime),
        udfChangeDateFormat(col(Dic.colOrderEndTime)).cast(StringType).as(Dic.colOrderEndTime))

    df_order
  }

  /**
    * Save order data.
    * @param spark
    * @param df_order
    */
  def saveOrder(spark: SparkSession, df_order: DataFrame) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.t_sdu_user_order_history_paypredict(
        |         user_id string,
        |         money double,
        |         resource_type double,
        |         resource_id string,
        |         resource_title string,
        |         creation_time string,
        |         discount_description string,
        |         order_status double,
        |         order_start_time string,
        |         order_end_time string)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_order.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_sdu_user_order_history_paypredict
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
