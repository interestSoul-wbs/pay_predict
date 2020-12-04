package predict.userpay

import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetDays}
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession

object UserProfileGenerateOrderPartForUserpay {

  val timeWindow = 30

  def main(args: Array[String]): Unit = {
    sysParamSetting()
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("userProfileGenerateOrderPartUserpayForPredict")
//      .master("local[4]")
      //      .enableHiveSupport()
      .getOrCreate()

    val now = args(0) + " " + args(1)
    println(now)

    userProfileGenerateOrderPart(spark, now)


  }

  def userProfileGenerateOrderPart(spark: SparkSession, now: String): Unit = {


    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val predictUserPath = hdfsPath + "data/predict/userpay/predictUsers" + now.split(" ")(0)
    val userProfileOrderPartSavePath = hdfsPath + "data/predict/common/processed/userpay/userprofileorderpart" + now.split(" ")(0)


    /**
     * Get Data
     */
    val df_orders = getData(spark, ordersProcessedPath)
    printDf("输入 df_orders", df_orders)

    val df_predict_users = getData(spark, predictUserPath)
    printDf("输入 df_predict", df_predict_users)

    val df_predict_id = df_predict_users.select(Dic.colUserId)


    val pre_30 = calDate(now, -30)
    val joinKeysUserId = Seq(Dic.colUserId)
    val df_predict_order = df_orders.filter(col(Dic.colCreationTime).<(now) and (col(Dic.colCreationTime) < calDate(now, -timeWindow * 3)))
      .join(df_predict_id, Seq(Dic.colUserId), "inner")

    val df_order_part_1 = df_predict_order
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

    val df_order_part_2 = df_predict_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneySinglesPurchased)
      )

    val df_order_part_3 = df_predict_order
      .filter(
        col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyConsumption)
      )

    val df_order_part_4 = df_predict_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneyPackagesUnpurchased)
      )

    val df_order_part_5 = df_predict_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneySinglesUnpurchased)
      )
    val df_order_part_6 = df_predict_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastPurchasePackage)
      )

    val df_order_part_7 = df_predict_order
      .filter(
        col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastClickPackage)
      )

    val df_order_part_8 = df_predict_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumbersOrdersLast30Days)
      )

    val df_order_part_9 = df_predict_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )
    val df_order_part_10 = df_predict_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )
    val df_order_part_11 = df_predict_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).===(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidSingleLast30Days)
      )

    val df_order_part_12 = df_predict_order
      .filter(
        col(Dic.colOrderEndTime).>(now)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)), lit(now)).as(Dic.colDaysRemainingPackage)
      )


    //当前是否是连续包月

    val df_user_profile_order = df_predict_id.join(df_order_part_1, joinKeysUserId, "left")
      .join(df_order_part_2, joinKeysUserId, "left")
      .join(df_order_part_3, joinKeysUserId, "left")
      .join(df_order_part_4, joinKeysUserId, "left")
      .join(df_order_part_5, joinKeysUserId, "left")
      .join(df_order_part_6, joinKeysUserId, "left")
      .join(df_order_part_7, joinKeysUserId, "left")
      .join(df_order_part_8, joinKeysUserId, "left")
      .join(df_order_part_9, joinKeysUserId, "left")
      .join(df_order_part_10, joinKeysUserId, "left")
      .join(df_order_part_11, joinKeysUserId, "left")
      .join(df_order_part_12, joinKeysUserId, "left")

    printDf("输出 df_user_profile_order", df_user_profile_order)


//    saveProcessedData(df_user_profile_order, userProfileOrderPartSavePath)


  }


}
