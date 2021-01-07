package common

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.GetSaveData.{getProcessedOrder, getProcessedPlay, saveUserProfileOrderPart}
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserProfileGenerateOrderPart {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var date: DateTime = _
  var nDaysFromStartDate: Int = _
  var dataSplitDate: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // union1.x
    sector = args(3).toInt
    nDaysFromStartDate = args(4).toInt // 1/7/14 - 各跑一次

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    dataSplitDate = (date - (30 - nDaysFromStartDate).days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    // 训练集的划分时间点 - 0 - 输入时间的30天之前
    // 单点视频训练集的划分时间点 - 7 - 输入时间的23天之前
    // 训练集的划分时间点 - 14 - 输入时间的16天之前
    println("data time is : " + dataSplitDate)

    // 1 - get play data.
    val df_plays = getProcessedPlay(partitiondate, license, vodVersion, sector)

    printDf("df_plays", df_plays)

    // 2 - get order data.
    val df_orders = getProcessedOrder(partitiondate, license, vodVersion, sector)

    printDf("df_orders", df_orders)

    // 3 - data process
    val df_result = userProfileGenerateOrderPart(dataSplitDate, 30, df_plays, df_orders)

    printDf("df_result", df_result)

    // 4 - save
    saveUserProfileOrderPart(df_result, partitiondate, license, vodVersion, sector, nDaysFromStartDate)
  }

  def userProfileGenerateOrderPart(now: String, timeWindow: Int, df_plays: DataFrame, df_orders: DataFrame) = {

    val df_user_id = df_plays
      .select(col(Dic.colUserId)).distinct()

    val pre_30 = calDate(now, -30)

    val joinKeysUserId = Seq(Dic.colUserId)

    val user_order = df_orders
      .filter(col(Dic.colCreationTime).<(now))

    val order_part_1 = user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyPackagesPurchased),
        max(col(Dic.colMoney)).as(Dic.colMaxMoneyPackagePurchased),
        min(col(Dic.colMoney)).as(Dic.colMinMoneyPackagePurchased),
        avg(col(Dic.colMoney)).as(Dic.colAvgMoneyPackagePurchased),
        stddev(col(Dic.colMoney)).as(Dic.colVarMoneyPackagePurchased))

    val order_part_2 = user_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneySinglesPurchased))

    val order_part_3 = user_order
      .filter(
        col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyConsumption))

    val order_part_4 = user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).<=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneyPackagesUnpurchased))

    val order_part_5 = user_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).<=(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneySinglesUnpurchased))

    val order_part_6 = user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastPurchasePackage))

    val order_part_7 = user_order
      .filter(
        col(Dic.colResourceType).>(0))
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastClickPackage))

    val order_part_8 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumbersOrdersLast30Days))

    val order_part_9 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days))

    val order_part_10 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).>(0))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days))

    val order_part_11 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).===(0))
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidSingleLast30Days))

    val order_part_12 = user_order
      .filter(
        col(Dic.colOrderEndTime).>(now)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1))
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)), lit(now)).as(Dic.colDaysRemainingPackage))

    val df_result = df_user_id
      .join(order_part_1, joinKeysUserId, "left")
      .join(order_part_2, joinKeysUserId, "left")
      .join(order_part_3, joinKeysUserId, "left")
      .join(order_part_4, joinKeysUserId, "left")
      .join(order_part_5, joinKeysUserId, "left")
      .join(order_part_6, joinKeysUserId, "left")
      .join(order_part_7, joinKeysUserId, "left")
      .join(order_part_8, joinKeysUserId, "left")
      .join(order_part_9, joinKeysUserId, "left")
      .join(order_part_10, joinKeysUserId, "left")
      .join(order_part_11, joinKeysUserId, "left")
      .join(order_part_12, joinKeysUserId, "left")

    df_result
  }

}
