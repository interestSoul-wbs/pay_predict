package train.userpay

import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetDays}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object UserProfileGenerateOrderPartForUserpay {
  val timeWindow = 30

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGenerateOrderPartForUserpayTrain")
      //.master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()


    val now = args(0) + " " + args(1)
    userProfileGenerateOrderPart(spark, now)

  }

  def userProfileGenerateOrderPart(spark: SparkSession, now: String): Unit = {

    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""

    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val trainUsersPath = hdfsPath + "data/train/userpay/trainUsers" + now.split(" ")(0)

    val userProfileOrderPartSavePath = hdfsPath + "data/train/common/processed/userpay/userprofileorderpart" + now.split(" ")(0)

    /**
     * Get Data
     */
    val df_orders = getData(spark, ordersProcessedPath)
    printDf("输入 df_orders", df_orders)

    val df_train_users = getData(spark, trainUsersPath)
    printDf("输入 df_train_users", df_train_users)


    val df_train_id = df_train_users.select(Dic.colUserId)


    val pre_30 = calDate(now, -30)

    val joinKeysUserId = Seq(Dic.colUserId)

    // 选取订单为训练时间前三个月的数据
    val df_train_order = df_orders.filter(col(Dic.colCreationTime).<(now) and (col(Dic.colCreationTime)) < calDate(now, -timeWindow * 3))
      .join(df_train_id, Seq(Dic.colUserId), "inner")
    /**
     * 已支付套餐数量 金额总数 最大金额 最小金额 平均金额 并对金额进行标准化
     */
    val df_order_part_1 = df_train_order
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
    /**
     * 单点视频
     */
    val df_order_part_2 = df_train_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesPurchased),
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneySinglesPurchased)
      )
    /**
     * 已购买的所有订单金额
     */
    val df_order_part_3 = df_train_order
      .filter(
        col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        sum(col(Dic.colMoney)).as(Dic.colTotalMoneyConsumption)
      )
    /**
     * 未购买套餐
     */
    val df_order_part_4 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPackagesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneyPackagesUnpurchased)
      )
    /**
     * 未购买单点
     */
    val df_order_part_5 = df_train_order
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).<=(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberSinglesUnpurchased),
        sum(col(Dic.colMoney)).as(Dic.colMoneySinglesUnpurchased)
      )

    /**
     * 距离上次购买最大天数
     */

    val df_order_part_6 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastPurchasePackage)
      )
    /**
     * 距离上次点击套餐最大天数
     */
    val df_order_part_7 = df_train_order
      .filter(
        col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)), lit(now)).as(Dic.colDaysSinceLastClickPackage)
      )
    /**
     * 30天前产生的订单总数
     */
    val df_order_part_8 = df_train_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumbersOrdersLast30Days)
      )
    /**
     * 30天前支付的订单数
     */
    val df_order_part_9 = df_train_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )
    val df_order_part_10 = df_train_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )
    val df_order_part_11 = df_train_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).===(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidSingleLast30Days)
      )
    /**
     * 仍然有效的套餐
     */
    val df_order_part_12 = df_train_order
      .filter(
        col(Dic.colOrderEndTime).>(now)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)), lit(now)).as(Dic.colDaysRemainingPackage)
      )


    val df_user_profile_order = df_train_id.join(df_order_part_1, joinKeysUserId, "left")
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


    printDf("输出  df_user_profile_order", df_user_profile_order)

    saveProcessedData(df_user_profile_order, userProfileOrderPartSavePath)

    println("用户画像订单部分生成完毕。")

  }



}
