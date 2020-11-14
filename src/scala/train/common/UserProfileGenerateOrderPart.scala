package train.common

import mam.Dic
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._

object UserProfileGenerateOrderPart {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-09-01 00:00:00
    val now = "2020-09-01 00:00:00"

    // 1 - get play data.
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    printDf("df_plays", df_plays)

    // 2 - get order data.
    val df_orders = getProcessedOrder(spark, partitiondate, license)

    printDf("df_orders", df_orders)

    // 3 - data process
    val df_result = userProfileGenerateOrderPart(now, 30, df_plays, df_orders)

    printDf("df_result", df_result)

    // 4 - save
    // 可能修改 - 2020-11-11
    saveUserProfileOrderPart(spark, df_result, partitiondate, license, "train")
  }

  def userProfileGenerateOrderPart(now: String, timeWindow: Int, df_plays: DataFrame, df_orders: DataFrame) = {

    val df_user_id = df_plays
      .select(col(Dic.colUserId)).distinct()

    printDf("df_user_id", df_user_id)

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


  /**
    * Save data to hive.
    *
    * @param spark
    * @param df_result
    */
  def saveUserProfileOrderPart(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_profile_order_part(
        |         user_id string,
        |         number_packages_purchased long,
        |         total_money_packages_purchased double,
        |         max_money_package_purchased double,
        |         min_money_package_purchased double,
        |         avg_money_package_purchased double,
        |         var_money_package_purchased double,
        |         number_singles_purchased long,
        |         total_money_singles_purchased double,
        |         total_money_consumption double,
        |         number_packages_unpurchased long,
        |         money_packages_unpurchased double,
        |         number_singles_unpurchased long,
        |         money_singles_unpurchased double,
        |         days_since_last_purchase_package int,
        |         days_since_last_click_package int,
        |         number_orders_last_30_days long,
        |         number_paid_orders_last_30_days long,
        |         number_paid_package_last_30_days long,
        |         number_paid_single_last_30_days long,
        |         days_remaining_package int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_profile_order_part
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |    user_id,
         |    number_packages_purchased,
         |    total_money_packages_purchased,
         |    max_money_package_purchased,
         |    min_money_package_purchased,
         |    avg_money_package_purchased,
         |    var_money_package_purchased,
         |    number_singles_purchased,
         |    total_money_singles_purchased,
         |    total_money_consumption,
         |    number_packages_unpurchased,
         |    money_packages_unpurchased,
         |    number_singles_unpurchased,
         |    money_singles_unpurchased,
         |    days_since_last_purchase_package,
         |    days_since_last_click_package,
         |    number_orders_last_30_days,
         |    number_paid_orders_last_30_days,
         |    number_paid_package_last_30_days,
         |    number_paid_single_last_30_days,
         |    days_remaining_package
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
