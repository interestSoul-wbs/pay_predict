package train.common

import mam.Utils.{calDate, printDf, udfGetDays}
import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserProfileGenerateOrderPartForUserpay {

  def userProfileGenerateOrderPart(now:String,timeWindow:Int,medias_path:String,plays_path:String,orders_path:String,hdfsPath:String): Unit = {
     System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGenerateOrderPartForUserpayTrain")
      //.master("local[6]")
      .getOrCreate()


    //val medias = spark.read.format("parquet").load(medias_path)
    val plays = spark.read.format("parquet").load(plays_path)
    val orders = spark.read.format("parquet").load(orders_path)

    printDf("输入 plays",plays)
    printDf("输入 orders",orders)

    //全部用户
    val userListPath = hdfsPath + "data/train/userpay/allUsers"
    var result = spark.read.format("parquet").load(userListPath)
    printDf("全部用户: ", result)


    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    // 选取订单为训练时间前三个月的数据
    val user_order = orders.filter(col(Dic.colCreationTime).<(now) and(col(Dic.colCreationTime)) < calDate(now, - 30 * 3))

    /**
     * 已支付套餐数量 金额总数 最大金额 最小金额 平均金额 并对金额进行标准化
     */
    val order_part_1 = user_order
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
    val order_part_2 = user_order
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
    val order_part_3 = user_order
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
    val order_part_4 = user_order
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
    val order_part_5 = user_order
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
     *距离上次购买最大天数
     */

    val order_part_6 = user_order
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)),lit(now)).as(Dic.colDaysSinceLastPurchasePackage)
      )
    /**
     * 距离上次点击套餐最大天数
     */
    val order_part_7 = user_order
      .filter(
        col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colCreationTime)),lit(now)).as(Dic.colDaysSinceLastClickPackage)
      )
    /**
     *30天前产生的订单总数
     */
    val order_part_8 = user_order
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
    val order_part_9 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
        && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidOrdersLast30Days)
      )
    val order_part_10 = user_order
      .filter(
        col(Dic.colCreationTime).>=(pre_30)
          && col(Dic.colOrderStatus).>(1)
        && col(Dic.colResourceType).>(0)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        count(col(Dic.colUserId)).as(Dic.colNumberPaidPackageLast30Days)
      )
    val order_part_11 = user_order
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
    val order_part_12 = user_order
      .filter(
        col(Dic.colOrderEndTime).>(now)
        && col(Dic.colResourceType).>(0)
        && col(Dic.colOrderStatus).>(1)
      )
      .groupBy(col(Dic.colUserId))
      .agg(
        udfGetDays(max(col(Dic.colOrderEndTime)),lit(now)).as(Dic.colDaysRemainingPackage)
      )


    result=result.join(order_part_1,joinKeysUserId,"left")
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


    printDf("输出  userprofileOrderPart", result)
    val userProfileOrderPartSavePath=hdfsPath+"data/train/common/processed/userpay/userprofileorderpart"+now.split(" ")(0)
    //大约有85万用户
    result.write.mode(SaveMode.Overwrite).format("parquet").save(userProfileOrderPartSavePath)




  }


    def main(args:Array[String]): Unit ={
      val hdfsPath="hdfs:///pay_predict/"
      //val hdfsPath=""

      val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
      val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3" //userpay
      val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders" //userpay


      val now=args(0)+" "+args(1)
      userProfileGenerateOrderPart(now,30,mediasProcessedPath,playsProcessedPath,ordersProcessedPath,hdfsPath)








  }

}
