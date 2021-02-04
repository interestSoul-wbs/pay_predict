package app.december.common

/**
  * Created by Konverse - 2021-01-07.
  */

import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import rs.common.SparkSessionInit

object OrdersProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var category: String = _
  var processedMediasData: String = _
  var date: DateTime = _
  var halfYearAgo: String = _

  def main(args: Array[String]): Unit = {

    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // 2020-12-1 - union1.x
    sector = args(3).toInt // 用户分群后的群代号
    category = args(4)
    processedMediasData = args(5)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get all raw order data - from partitiondate to half year ago.
    val df_user_info = getAllRawUserInfos(processedMediasData, license, vodVersion, sector)

    // 2 - join subscriberid
    val df_original_order = getRawOrderByDateRangeAllUsers(halfYearAgo, partitiondate, license)

    // 3 - order info
    val df_order_raw = df_user_info
      .join(df_original_order, Seq(Dic.colDeviceid, Dic.colCustomerid))
      .drop(Dic.colDeviceid, Dic.colCustomerid)
      .withColumnRenamed(Dic.colSubscriberid, Dic.colUserId)

    // 4 - process of order data.
    val df_order_processed = orderProcees(df_order_raw)

    // 5 - save data to hive.
    saveProcessedOrderV2(df_order_processed, partitiondate, license, vodVersion, sector, category)
  }


  def orderProcees(df_order_raw: DataFrame) = {

    val df_order_processed_tmp_1 = df_order_raw
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
      .withColumn(Dic.colCreationTime, udfChangeDateFormat(col(Dic.colCreationTime)))
      .withColumn(Dic.colOrderStartTime, udfChangeDateFormat(col(Dic.colOrderStartTime)))
      .withColumn(Dic.colOrderEndTime, udfChangeDateFormat(col(Dic.colOrderEndTime)))
      .select(
        when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
        when(col(Dic.colMoney) === "NULL", null).otherwise(col(Dic.colMoney) cast DoubleType).as(Dic.colMoney),
        when(col(Dic.colResourceType) === "NULL", null).otherwise(col(Dic.colResourceType) cast DoubleType).as(Dic.colResourceType),
        when(col(Dic.colResourceId) === "NULL", null).otherwise(col(Dic.colResourceId)).as(Dic.colResourceId),
        when(col(Dic.colResourceTitle) === "NULL", null).otherwise(col(Dic.colResourceTitle)).as(Dic.colResourceTitle),
        when(col(Dic.colCreationTime) === "NULL", null).otherwise(col(Dic.colCreationTime) cast TimestampType).as(Dic.colCreationTime),
        when(col(Dic.colDiscountDescription) === "NULL", null).otherwise(col(Dic.colDiscountDescription)).as(Dic.colDiscountDescription),
        when(col(Dic.colOrderStatus) === "NULL", null).otherwise(col(Dic.colOrderStatus) cast DoubleType).as(Dic.colOrderStatus),
        when(col(Dic.colOrderStartTime) === "NULL", null).otherwise(col(Dic.colOrderStartTime) cast TimestampType).as(Dic.colOrderStartTime),
        when(col(Dic.colOrderEndTime) === "NULL", null).otherwise(col(Dic.colOrderEndTime) cast TimestampType).as(Dic.colOrderEndTime))
      .dropDuplicates()
      .na.drop("all")
      //计算有效时长
      .withColumn(Dic.colTimeValidity, udfGetDays(col(Dic.colOrderEndTime), col(Dic.colOrderStartTime)))
      //选取有效时间大于0的
      .filter(col(Dic.colTimeValidity).>=(0))
      //统一有效时长
      .withColumn(Dic.colTimeValidity, udfUniformTimeValidity(col(Dic.colTimeValidity), col(Dic.colResourceType)))
      // 选取生效时间晚于 creation_time 的数据 ，由于存在1/4的创建)数据晚于生效时间，但时间差距基本为几秒，因此比较时间部分加上1min
      .withColumn(Dic.colKeepSign, udfGetKeepSign(col(Dic.colCreationTime), col(Dic.colOrderStartTime)))
      .filter(col(Dic.colKeepSign) === 1)
      .drop(Dic.colKeepSign)
      // 去掉同一用户 同一时间产生相同的订单异常
      // 1.部分数据生效时间差1秒
      // 2.同一时间产生的两个订单，一个支付成功，一个支付不成功，保留支付成功的订单信息
      .dropDuplicates(Dic.colUserId, Dic.colCreationTime, Dic.colResourceId, Dic.colOrderStatus, Dic.colOrderStartTime)
    //选取同时产生的两个订单中支付成功的(父子订单)

    val df_order_processed_tmp_2 = df_order_processed_tmp_1
      .groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime)
      .max(Dic.colOrderStatus)
      .withColumnRenamed("max(order_status)", Dic.colOrderStatus)

    val df_order_processed = df_order_processed_tmp_1
      .join(df_order_processed_tmp_2,
        Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus),
        "inner")
      .select(
        col(Dic.colUserId)
        , col(Dic.colResourceId)
        , col(Dic.colCreationTime).cast(StringType)
        , col(Dic.colOrderStartTime).cast(StringType)
        , col(Dic.colOrderStatus)
        , col(Dic.colMoney)
        , col(Dic.colResourceType)
        , col(Dic.colResourceTitle)
        , col(Dic.colDiscountDescription)
        , col(Dic.colOrderEndTime).cast(StringType)
        , col(Dic.colTimeValidity))

    df_order_processed
  }


}
