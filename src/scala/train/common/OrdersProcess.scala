package train.common

/**
  * @Author wj
  * @Date 2020/09
  * @Version 1.0
  */

import mam.Dic
import mam.Utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, when, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OrdersProcess {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrdersProcess")
      .master("local[6]")
      .getOrCreate()

    //hdfs:///pay_predict/
    import org.apache.spark.sql.functions._
    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val orderRawPath = hdfsPath + "data/train/common/raw/orders/order*.txt"
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders"
    val df_raw_order = getRawOrders(orderRawPath, spark)

    printDf("df_raw_order", df_raw_order)
    df_raw_order.filter(col(Dic.colUserId).===("106411")).show()

    // 2 - process of order data.
    val df_order = multiOrderTimesProcess(df_raw_order)

    printDf("df_order", df_order)

    val df_order_processed = orderProcess(df_order)

    printDf("df_order_processed", df_order_processed)
    df_order_processed.filter(col(Dic.colUserId).===("106411")).show()
    df_order_processed.write.mode(SaveMode.Overwrite).format("parquet").save(orderProcessedPath)
    println("订单数据处理完成！")
  }


  def getRawOrders(orderRawPath: String, spark: SparkSession) = {
    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colMoney, StringType),
        StructField(Dic.colResourceType, StringType),
        StructField(Dic.colResourceId, StringType),
        StructField(Dic.colResourceTitle, StringType),
        StructField(Dic.colCreationTime, StringType),
        StructField(Dic.colDiscountDescription, StringType),
        StructField(Dic.colOrderStatus, StringType),
        StructField(Dic.colOrderStartTime, StringType),
        StructField(Dic.colOrderEndTime, StringType)

      )
    )
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(orderRawPath)
    df

  }


  /**
    * Process of order data.
    *
    * @param df_raw_order
    * @return
    */
  def multiOrderTimesProcess(orderRaw: DataFrame) = {

    val df_order = orderRaw.withColumn(Dic.colCreationTime, udfChangeDateFormat(col(Dic.colCreationTime)))
      .withColumn(Dic.colOrderStartTime, udfChangeDateFormat(col(Dic.colOrderStartTime)))
      .withColumn(Dic.colOrderEndTime, udfChangeDateFormat(col(Dic.colOrderEndTime)))
      .select(
        when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
        when(col(Dic.colMoney) === "NULL", Double.NaN).otherwise(col(Dic.colMoney) cast DoubleType).as(Dic.colMoney),
        when(col(Dic.colResourceType) === "NULL", Double.NaN).otherwise(col(Dic.colResourceType) cast DoubleType).as(Dic.colResourceType),
        when(col(Dic.colResourceId) === "NULL", null).otherwise(col(Dic.colResourceId)).as(Dic.colResourceId),
        when(col(Dic.colResourceTitle) === "NULL", null).otherwise(col(Dic.colResourceTitle)).as(Dic.colResourceTitle),
        when(col(Dic.colCreationTime) === "NULL", null).otherwise(col(Dic.colCreationTime) cast TimestampType).as(Dic.colCreationTime),
        when(col(Dic.colDiscountDescription) === "NULL", null).otherwise(col(Dic.colDiscountDescription)).as(Dic.colDiscountDescription),
        when(col(Dic.colOrderStatus) === "NULL", Double.NaN).otherwise(col(Dic.colOrderStatus) cast DoubleType).as(Dic.colOrderStatus),
        when(col(Dic.colOrderStartTime) === "NULL", null).otherwise(col(Dic.colOrderStartTime) cast TimestampType).as(Dic.colOrderStartTime),
        when(col(Dic.colOrderEndTime) === "NULL", null).otherwise(col(Dic.colOrderEndTime) cast TimestampType).as(Dic.colOrderEndTime))

    df_order
  }

  def orderProcess(df_order: DataFrame) = {

    val orderProcessed = df_order
      .withColumn(Dic.colTimeValidity,udfGetDays(col(Dic.colOrderEndTime),col(Dic.colOrderStartTime)))
      //选取有效时间大于0的
      .filter(col(Dic.colTimeValidity).>=(0))
      // 根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
      .withColumn(Dic.colDiscountDescription, udfFillDiscountDescription(col(Dic.colResourceType),col(Dic.colTimeValidity)))
      .withColumn(Dic.colKeepSign, udfGetKeepSign(col(Dic.colCreationTime),col(Dic.colOrderStartTime)))
      .filter(col(Dic.colKeepSign) === 1)
      .drop(Dic.colKeepSign)
      .dropDuplicates(Dic.colUserId, Dic.colCreationTime, Dic.colResourceId, Dic.colOrderStatus, Dic.colOrderStartTime)

    val orderProcessed2 = orderProcessed
      .groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime)
      .agg(max(Dic.colOrderStatus).as(Dic.colOrderStatus))

    val df_order_processed = orderProcessed.join(orderProcessed2, Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus), "inner")

    df_order_processed
  }

}
