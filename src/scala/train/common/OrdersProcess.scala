package train.common

import mam.Dic
import mam.SparkSessionInit._
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode}
import mam.SparkSessionInit

object OrdersProcess {

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()


    // 2 數據讀取
    val df_order_raw = getRawOrders2(spark)
//    val df_order_raw = getRawOrders2(spark)
    printDf("输入 df_order_raw", df_order_raw)

    // 3 數據處理
    val df_order_processed = orderProcess(df_order_raw)
    printDf("输出 df_order_processed", df_order_processed)

    // 4 數據存儲
    saveProcessedOrder(df_order_processed)
    println("订单数据处理完成！")
  }


  def orderProcess(df_order_raw: DataFrame) = {

    val df_order_processed_tmp_1 = df_order_raw
      .withColumn(Dic.colCreationTime, udfChangeDateFormat(col(Dic.colCreationTime)))
      .withColumn(Dic.colOrderStartTime, udfChangeDateFormat(col(Dic.colOrderStartTime)))
      .withColumn(Dic.colOrderEndTime, udfChangeDateFormat(col(Dic.colOrderEndTime)))
      .select(
        when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
        when(col(Dic.colMoney) === "NULL", null).otherwise(col(Dic.colMoney).cast(DoubleType)).as(Dic.colMoney),
        when(col(Dic.colResourceType) === "NULL", null).otherwise(col(Dic.colResourceType).cast(DoubleType)).as(Dic.colResourceType),
        when(col(Dic.colResourceId) === "NULL", null).otherwise(col(Dic.colResourceId)).as(Dic.colResourceId),
        when(col(Dic.colResourceTitle) === "NULL", null).otherwise(col(Dic.colResourceTitle)).as(Dic.colResourceTitle),
        when(col(Dic.colCreationTime) === "NULL", null).otherwise(col(Dic.colCreationTime).cast(TimestampType)).as(Dic.colCreationTime),
        when(col(Dic.colDiscountDescription) === "NULL", null).otherwise(col(Dic.colDiscountDescription)).as(Dic.colDiscountDescription),
        when(col(Dic.colOrderStatus) === "NULL", null).otherwise(col(Dic.colOrderStatus).cast(DoubleType)).as(Dic.colOrderStatus),
        when(col(Dic.colOrderStartTime) === "NULL", null).otherwise(col(Dic.colOrderStartTime).cast(TimestampType)).as(Dic.colOrderStartTime),
        when(col(Dic.colOrderEndTime) === "NULL", null).otherwise(col(Dic.colOrderEndTime).cast(TimestampType)).as(Dic.colOrderEndTime))
      .dropDuplicates()
      .na.drop("all")
      .na.drop(Seq(Dic.colUserId, Dic.colResourceId, Dic.colOrderEndTime, Dic.colOrderStartTime))
      //计算有效时长
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
    //选取同时产生的两个订单中支付成功的(父子订单)


    printDf("df_order_processed_tmp_1", df_order_processed_tmp_1)

    val df_order_processed_tmp_2 = df_order_processed_tmp_1
      .groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime)
      .max(Dic.colOrderStatus)
      .withColumnRenamed("max(order_status)", Dic.colOrderStatus)

    printDf("df_order_processed_tmp_2", df_order_processed_tmp_2)

    val df_order_processed = df_order_processed_tmp_1
      .join(df_order_processed_tmp_2,
        Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus),
        "inner")

    df_order_processed
  }
}
