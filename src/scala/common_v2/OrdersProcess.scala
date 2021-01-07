package common_v2

/**
  * Konverse - 2020-11-30.
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
  var date: DateTime = _
  var halfYearAgo: String = _

  def main(args: Array[String]): Unit = {

    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // 2020-12-1 - union1.x
    sector = args(3).toInt // 用户分群后的群代号

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    halfYearAgo = (date - 180.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    // 1 - get all raw order data - from partitiondate to half year ago.
    val df_user_info = getAllRawUserInfos(partitiondate, license, vodVersion, sector)

    printDf("df_user_info", df_user_info)

    // 2 - join subscriberid
    val df_original_order = getRawOrderByDateRangeAllUsers(halfYearAgo, partitiondate, license)

    printDf("df_original_order", df_original_order)

    // 3 - order info
    val df_raw_order = df_user_info
      .join(df_original_order, Seq(Dic.colDeviceid, Dic.colCustomerid))
      .drop(Dic.colDeviceid, Dic.colCustomerid)
      .withColumnRenamed(Dic.colSubscriberid, Dic.colUserId)

    printDf("df_raw_order", df_raw_order)

    // 4 - process of order data.
    val df_order = multiOrderTimesProcess(df_raw_order)

    printDf("df_order", df_order)

    val df_order_processed = orderProcees(df_order)

    printDf("df_order_processed", df_order_processed)

    // 5 - save data to hive.
    saveProcessedOrderV2(df_order_processed, partitiondate, license, vodVersion, sector)
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

    val df_order_processed_tmp_1 = df_order
      .dropDuplicates()
      .na.drop()
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

      .dropDuplicates(Dic.colUserId, Dic.colCreationTime, Dic.colResourceId, Dic.colOrderStatus, Dic.colOrderStartTime)

    val df_order_processed_tmp_2 = df_order_processed_tmp_1
      .groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime)
      .agg(max(Dic.colOrderStatus).as(Dic.colOrderStatus))

    val df_order_processed = df_order_processed_tmp_1
      .join(df_order_processed_tmp_2, Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus), "inner")

    df_order_processed
  }


}
