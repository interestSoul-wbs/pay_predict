package train.common

/**
  * @Author wj
  * @Date 2020/09
  * @Version 1.0
  */

import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrdersProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var halfYearAgo: String = _

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get all raw order data.
    val df_raw_order = getRawOrderByDateRangeSmpleUsers(spark, halfYearAgo, partitiondate, license)

    printDf("df_raw_order", df_raw_order)

    // 2 - process of order data.
    val df_order = multiOrderTimesProcess(df_raw_order)

    printDf("df_order", df_order)

    val df_order_processed = orderProcees(df_order)

    printDf("df_order_processed", df_order_processed)

    // 3 - save data to hive.
    saveProcessedOrder(spark, df_order_processed)

    println("预测阶段订单数据处理完成！")
  }

  /**
    * Process of order data.
    *
    * @param df_raw_order
    * @return
    */
  def multiOrderTimesProcess(df_raw_order: DataFrame) = {

    val df_order = df_raw_order
      .na.drop(Array(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderEndTime))
      .withColumn(Dic.colIsOnlyNumberUserId, udfIsOnlyNumber(col(Dic.colUserId)))
      .withColumn(Dic.colIsOnlyNumberResourceId, udfIsOnlyNumber(col(Dic.colResourceId)))
      .withColumn(Dic.colIsLongtypeTimeCreationTime, udfIsLongTypeTimePattern2(col(Dic.colCreationTime)))
      .withColumn(Dic.colIsLongtypeTimeOrderStartTime, udfIsLongTypeTimePattern2(col(Dic.colOrderStartTime)))
      .withColumn(Dic.colIsLongtypeTimeOrderEndTime, udfIsLongTypeTimePattern2(col(Dic.colOrderEndTime)))
      .filter(
        col(Dic.colIsOnlyNumberUserId).===(1)
          && col(Dic.colIsOnlyNumberResourceId).===(1)
          && col(Dic.colIsLongtypeTimeCreationTime).===(1)
          && col(Dic.colIsLongtypeTimeOrderStartTime).===(1)
          && col(Dic.colIsLongtypeTimeOrderEndTime).===(1))
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

  def orderProcees(df_order: DataFrame) = {

    val orderProcessed = df_order
      .withColumn(Dic.colTimeValidity, udfGetDays(col(Dic.colOrderEndTime), col(Dic.colOrderStartTime)))
      //选取有效时间大于0的
      .filter(col(Dic.colTimeValidity).>=(0))
      // 根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
      .withColumn(Dic.colDiscountDescription, udfFillDiscountDescription(col(Dic.colResourceType), col(Dic.colTimeValidity)))
      .withColumn(Dic.colKeepSign, udfGetKeepSign(col(Dic.colCreationTime), col(Dic.colOrderStartTime)))
      .filter(col(Dic.colKeepSign) === 1)
      .drop(Dic.colKeepSign)
      .dropDuplicates(Dic.colUserId, Dic.colCreationTime, Dic.colResourceId, Dic.colOrderStatus, Dic.colOrderStartTime)

    val orderProcessed2 = orderProcessed
      .groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime)
      .agg(max(Dic.colOrderStatus).as(Dic.colOrderStatus))

    val df_order_processed = orderProcessed.join(orderProcessed2, Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus), "inner")

    df_order_processed
  }


  /**
    * Save order data.
    *
    * @param spark
    * @param df_order
    */
  def saveProcessedOrder(spark: SparkSession, df_order: DataFrame) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_order(
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
         |    vodrs.paypredict_processed_order
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
