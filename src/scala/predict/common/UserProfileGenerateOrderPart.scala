package predict.common

import mam.GetSaveData.{getProcessedOrder, saveUserProfileOrderPart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfGetDays}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UserProfileGenerateOrderPart {

  val timeWindow = 30


  def main(args:Array[String]): Unit ={

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)

    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    val df_user_profile_order=userProfileGenerateOrderPart(now,timeWindow,df_orders)



    // 4 Save Data
    saveUserProfileOrderPart(now, df_user_profile_order,"predict")
    printDf("输出  df_user_profile_order", df_user_profile_order)

    println("UserProfileGenerateOrderPart  over~~~~~~~~~~~\")")



  }

  def userProfileGenerateOrderPart( now:String, timeWindow:Int, df_orders:DataFrame) = {

    val df_order_users = df_orders.select(col(Dic.colUserId)).distinct()


    val pre_30 = calDate(now, -timeWindow)
    val joinKeysUserId = Seq(Dic.colUserId)

    val user_order=df_orders.filter(col(Dic.colCreationTime).<(now))
    val order_part_1=user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyPackagesPurchased),
        max(col(Dic.colMoney)).as(Dic.colMaxMoneyPackagePurchased),
        min(col(Dic.colMoney)).as(Dic.colMinMoneyPackagePurchased),
        avg(col(Dic.colMoney)).as(Dic.colAvgMoneyPackagePurchased),
        stddev(col(Dic.colMoney)).as(Dic.colVarMoneyPackagePurchased)
      )
    val order_part_2=user_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneySinglesPurchased)
      )
    val order_part_3=user_order
      .filter(
        col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyConsumption)
      )
    val order_part_4=user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneyPackagesUnpurchased)
      )

    val order_part_5=user_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneySinglesUnpurchased)
      )
    val order_part_6=user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)),lit(now)).as(Dic.colDaysSinceLastPurchasePackage)
      )

    val order_part_7=user_order
      .filter(
        col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)),lit(now)).as(Dic.colDaysSinceLastClickPackage)
      )

    val order_part_8=user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumbersOrdersLast30Days)
      )

    val order_part_9=user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
        && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )
    val order_part_10=user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
        && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )
    val order_part_11=user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).===(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidSingleLast30Days)
      )

    val order_part_12=user_order
      .filter(
        col(Dic.colOrderEndTime).>(now)
        && col(Dic.colResourceType).>(0)
        && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)),lit(now)).as(Dic.colDaysRemainingPackage)
      )





    val df_user_profile_order=df_order_users.join(order_part_1,joinKeysUserId,"left")
    .join(order_part_2,joinKeysUserId, "left")
    .join(order_part_3,joinKeysUserId,"left")
    .join(order_part_4,joinKeysUserId, "left")
    .join(order_part_5,joinKeysUserId, "left")
    .join(order_part_6,joinKeysUserId, "left")
    .join(order_part_7,joinKeysUserId, "left")
    .join(order_part_8,joinKeysUserId,"left")
    .join(order_part_9,joinKeysUserId, "left")
    .join(order_part_10,joinKeysUserId, "left")
    .join(order_part_11,joinKeysUserId, "left")
    .join(order_part_12,joinKeysUserId, "left")

     df_user_profile_order





  }


}
