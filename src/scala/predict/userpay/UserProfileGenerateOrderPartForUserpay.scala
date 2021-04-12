package predict.userpay

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getPredictUser, getProcessedOrder, saveProcessedData, saveUserProfileOrderPart}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetDays, udfGetHour, udfIsMemberCurrent}
import org.apache.spark.sql.functions._
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserProfileGenerateOrderPartForUserpay {

  val timeWindow = 30

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)
    println(now)

    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    val df_predict_users = getPredictUser(spark, now)
    printDf("输入 df_predict_users", df_predict_users)

    // 3 Process Data
    val df_user_profile_order = userProfileGenerateOrderPart(spark, now, df_orders, df_predict_users)

    // 4 Save Data
    saveUserProfileOrderPart(now, df_user_profile_order, "predict")
    printDf("输出  df_user_profile_order", df_user_profile_order)
    println("用户画像Order部分生成完毕。")


  }

  def userProfileGenerateOrderPart(spark: SparkSession, now: String, df_orders: DataFrame, df_predict_users:DataFrame): DataFrame = {


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
        avg(col(Dic.colMoney)).as(Dic.colAvgMoneyPackagePurchased)
//        stddev(col(Dic.colMoney)).as(Dic.colVarMoneyPackagePurchased)
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


    //用户当前是否是会员
        val df_order_part_13=df_predict_order
          .filter(
            col(Dic.colResourceType).>(0)
              && col(Dic.colOrderStatus).>(1)
          )
          .groupBy(col(Dic.colUserId))
          .agg(
            udfIsMemberCurrent(max(col(Dic.colOrderEndTime)),lit(now)).as(Dic.colIsMemberCurrent)
          )
        //用户在未来一周内会员是否过期
        val df_order_part_14=df_predict_order
          .filter(
            col(Dic.colResourceType).>(0)
              && col(Dic.colOrderStatus).>(1)
          )
          .groupBy(col(Dic.colUserId))
          .agg(
            udfIsMemberCurrent(max(col(Dic.colOrderEndTime)),lit(calDate(now,7))).as(Dic.colIsMember7Days)
          )
        //用户在未来两周内会员是否过期
        val df_order_part_15=df_predict_order
          .filter(
            col(Dic.colResourceType).>(0)
              && col(Dic.colOrderStatus).>(1)
          )
          .groupBy(col(Dic.colUserId))
          .agg(
            udfIsMemberCurrent(max(col(Dic.colOrderEndTime)),lit(calDate(now,14))).as(Dic.colIsMember14Days)
          )






        //用户经常在哪个时段下单
        val df_order_part_16=df_predict_order
          .filter(
            col(Dic.colResourceType).>(0)
              && col(Dic.colOrderStatus).>(1)
          )
          .withColumn(Dic.colOrderHour,udfGetHour(col(Dic.colCreationTime)))
          .groupBy(col(Dic.colUserId))
          .agg(
            mean(col(Dic.colOrderHour)).as(Dic.colOrderHour)
          )
        //用户经常周几下单


        //用户创建套餐订单的间隔
        val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colCreationTime)
        val order_part_temp = df_predict_order
          .filter(
            col(Dic.colResourceType).>(0)
              && col(Dic.colOrderStatus).>(1)
          )
          .withColumn("second_order_time", lead(Dic.colCreationTime, 1).over(win1)) //下一个start_time
          .withColumn("time_gap",udfGetDays(col(Dic.colCreationTime),col("second_order_time")))

        order_part_temp.show()

        val df_order_part_17=order_part_temp
          .groupBy(col(Dic.colUserId))
          .agg(
            sum(col("time_gap")).divide(count(col(Dic.colCreationTime))-1).as(Dic.colMeanGap)
          )




    val df_user_profile_order = df_predict_id.join(df_order_part_1, joinKeysUserId, "left")
//      .join(df_order_part_2, joinKeysUserId, "left")
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
//          .join(df_order_part_13,joinKeysUserId, "left")
//          .join(df_order_part_14,joinKeysUserId, "left")
//          .join(df_order_part_15,joinKeysUserId, "left")
//          .join(df_order_part_16,joinKeysUserId, "left")
//          .join(df_order_part_17,joinKeysUserId, "left")



    df_user_profile_order
  }


}
