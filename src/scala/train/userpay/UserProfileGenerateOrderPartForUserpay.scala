package train.userpay

import mam.Dic
import mam.Utils.{calDate, getData, printDf, saveProcessedData, udfGetDays}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object UserProfileGenerateOrderPartForUserpay {

  def userProfileGenerateOrderPart(now: String, df_orders: DataFrame, df_trainUsers: DataFrame, userProfileOrderPartSavePath: String): Unit = {


    val df_trainId = df_trainUsers.select(Dic.colUserId)


    val pre_30 = calDate(now, -30)

    val joinKeysUserId = Seq(Dic.colUserId)

    // 选取订单为训练时间前三个月的数据
    val df_trainUserOrder = df_orders.filter(col(Dic.colCreationTime).<(now) and (col(Dic.colCreationTime)) < calDate(now, -30 * 3))
      .join(df_trainId, Seq(Dic.colUserId), "inner")
    /**
     * 已支付套餐数量 金额总数 最大金额 最小金额 平均金额 并对金额进行标准化
     */
    val order_part_1 = df_trainUserOrder
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
    val order_part_2 = df_trainUserOrder
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
    val order_part_3 = df_trainUserOrder
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
    val order_part_4 = df_trainUserOrder
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
    val order_part_5 = df_trainUserOrder
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

    val order_part_6 = df_trainUserOrder
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
    val order_part_7 = df_trainUserOrder
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
    val order_part_8 = df_trainUserOrder
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
    val order_part_9 = df_trainUserOrder
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )
    val order_part_10 = df_trainUserOrder
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
          && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )
    val order_part_11 = df_trainUserOrder
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
    val order_part_12 = df_trainUserOrder
      .filter(
        col(Dic.colOrderEndTime).>(now)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)), lit(now)).as(Dic.colDaysRemainingPackage)
      )


    var df_trainUserProfileOrder = df_trainId.join(order_part_1, joinKeysUserId, "left")
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


    printDf("输出  userprofileOrderPart", df_trainUserProfileOrder)

    //大约有85万用户
    saveProcessedData(df_trainUserProfileOrder, userProfileOrderPartSavePath)

  }


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGenerateOrderPartForUserpayTrain")
      .master("local[6]")
      .getOrCreate()


    val now = args(0) + " " + args(1)

    //val hdfsPath = "hdfs:///pay_predict/"
    val hdfsPath = ""

    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3" //userpay
    val trainUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)
    val userProfileOrderPartSavePath = hdfsPath + "data/train/common/processed/userpay/userprofileorderpart" + now.split(" ")(0)

    /**
     * Get Data
     */
    val df_orders = getData(spark, ordersProcessedPath)
    val df_trainUsers = getData(spark, trainUsersPath)


    userProfileGenerateOrderPart(now, df_orders, df_trainUsers, userProfileOrderPartSavePath)


  }

}
