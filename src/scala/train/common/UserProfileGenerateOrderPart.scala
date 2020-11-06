package train.common

import mam.Utils.{calDate, printDf, udfGetDays}
import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserProfileGenerateOrderPart {

  def userProfileGenerateOrderPart(now:String,timeWindow:Int,medias_path:String,plays_path:String,orders_path:String,hdfsPath:String): Unit = {
     System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileGenerateOrderPart")
      //.master("local[6]")
      .getOrCreate()
    //设置shuffle过程中分区数
    // spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1000")
    import org.apache.spark.sql.functions._

    //val medias = spark.read.format("parquet").load(medias_path)
    val plays = spark.read.format("parquet").load(plays_path)
    val orders = spark.read.format("parquet").load(orders_path)

    printDf("输入 plays",plays)
    printDf("输入 orders",orders)


    var result = plays.select(col(Dic.colUserId)).distinct()


    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysUserId = Seq(Dic.colUserId)

    val user_order=orders.filter(col(Dic.colCreationTime).<(now))
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

   // result49.show()
    printDf("输出  userprofileOrderPart",result)
    val userProfileOrderPartSavePath=hdfsPath+"data/train/common/processed/userprofileorderpart"+now.split(" ")(0)
    //大约有85万用户
    result.write.mode(SaveMode.Overwrite).format("parquet").save(userProfileOrderPartSavePath)




  }


    def main(args:Array[String]): Unit ={
      val hdfsPath="hdfs:///pay_predict/"
      //val hdfsPath=""
      val mediasProcessedPath=hdfsPath+"data/train/common/processed/mediastemp"
      val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
      val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
      val now=args(0)+" "+args(1)
      userProfileGenerateOrderPart(now,30,mediasProcessedPath,playsProcessedPath,ordersProcessedPath,hdfsPath)








  }

}
