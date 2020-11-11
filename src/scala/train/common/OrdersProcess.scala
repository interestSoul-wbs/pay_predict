package train.common
/**
 * @Author wj
 * @Date 2020/09
 * @Version 1.0
 */

import java.text.SimpleDateFormat

import mam.Dic
import mam.Utils
import mam.Utils.{printDf, udfChangeDateFormat, udfFillDiscountDescription, udfGetDays, udfGetErrorMoneySign, udfGetKeepSign, udfUniformTimeValidity}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import shapeless.ops.tuple

object OrdersProcess {

    def main(args: Array[String]): Unit ={
      System.setProperty("hadoop.home.dir","c:\\winutils")
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark: SparkSession = new sql.SparkSession.Builder()
        .appName("OrdersProcess")
        .master("local[6]")
        .getOrCreate()

       //hdfs:///pay_predict/
      import org.apache.spark.sql.functions._
      //val hdfsPath="hdfs:///pay_predict/"
      val hdfsPath=""
      val orderRawPath=hdfsPath+"data/train/common/raw/orders/order*.txt"
      val orderProcessedPath=hdfsPath+"data/train/common/processed/orders"
      val orderRaw=getRawOrders(orderRawPath,spark)

      printDf("输入 orderRaw",orderRaw)

      val orderProcessed = orderProcess(orderRaw)

//      printDf("输出 orderProcessed",orderProcessed)
//      orderProcessed.filter(col(Dic.colUserId).===("106411")).show()
//
//      orderProcessed.write.mode(SaveMode.Overwrite).format("parquet").save(orderProcessedPath)
//      println("订单数据处理完成！")
    }


  def getRawOrders(orderRawPath:String,spark:SparkSession)={
    val schema= StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colMoney, StringType),
        StructField(Dic.colResourceType, StringType),
        StructField(Dic.colResourceId, StringType),
        StructField(Dic.colResourceTitle, StringType),
        StructField(Dic.colCreationTime, StringType),
        StructField(Dic.colDiscountDescription, StringType),
        StructField(Dic.colOrderStatus, StringType),
        StructField(Dic.colOrderStartTime, StringType),
        StructField(Dic.colOrderEndTime, StringType)

      )
    )
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(orderRawPath)
    df

  }
  def orderProcess(orderRaw:DataFrame)={
    var orderProcessed = orderRaw.withColumn(Dic.colCreationTime,udfChangeDateFormat(col(Dic.colCreationTime)))
      .withColumn(Dic.colOrderStartTime,udfChangeDateFormat(col(Dic.colOrderStartTime)))
      .withColumn(Dic.colOrderEndTime,udfChangeDateFormat(col(Dic.colOrderEndTime)))

    orderProcessed = orderProcessed.select(
      when(col(Dic.colUserId)==="NULL",null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
      when(col(Dic.colMoney)==="NULL",Double.NaN).otherwise(col(Dic.colMoney) cast DoubleType).as(Dic.colMoney),
      when(col(Dic.colResourceType)==="NULL",Double.NaN).otherwise(col(Dic.colResourceType) cast DoubleType).as(Dic.colResourceType),
      when(col(Dic.colResourceId)==="NULL",null).otherwise(col(Dic.colResourceId) ).as(Dic.colResourceId),
      when(col(Dic.colResourceTitle)==="NULL",null).otherwise(col(Dic.colResourceTitle)).as(Dic.colResourceTitle),
      when(col(Dic.colCreationTime)==="NULL",null).otherwise(col(Dic.colCreationTime) cast TimestampType ).as(Dic.colCreationTime),
      when(col(Dic.colDiscountDescription)==="NULL",null).otherwise(col(Dic.colDiscountDescription)).as(Dic.colDiscountDescription),
      when(col(Dic.colOrderStatus)==="NULL",Double.NaN).otherwise(col(Dic.colOrderStatus) cast DoubleType).as(Dic.colOrderStatus),
      when(col(Dic.colOrderStartTime)==="NULL",null).otherwise(col(Dic.colOrderStartTime) cast TimestampType).as(Dic.colOrderStartTime),
      when(col(Dic.colOrderEndTime)==="NULL",null).otherwise(col(Dic.colOrderEndTime) cast TimestampType).as(Dic.colOrderEndTime)
    )
    /**
     * 添加订单的有效时长 选取有效时长大于0的订单   有效时长（单位 天） 并填充打折描述的空值
     */
    //计算有效时长
    orderProcessed = orderProcessed.withColumn(Dic.colTimeValidity,udfGetDays(col(Dic.colOrderEndTime),col(Dic.colOrderStartTime)))
      //选取有效时间大于0的
      .filter(col(Dic.colTimeValidity).>=(0))
      // 根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
      //.withColumn(Dic.colDiscountDescription, udfFillDiscountDescription(col(Dic.colResourceType),col(Dic.colTimeValidity)))
      //统一有效时长
      .withColumn(Dic.colTimeValidity, udfUniformTimeValidity(col(Dic.colTimeValidity)))


    /**
     * 选取生效时间晚于 creation_time 的数据 ，由于存在1/4的创建数据晚于生效时间，但时间差距基本为几秒，因此比较时间部分加上1min
     */

    orderProcessed= orderProcessed.withColumn(Dic.colKeepSign, udfGetKeepSign(col(Dic.colCreationTime),col(Dic.colOrderStartTime)))
      .filter(col(Dic.colKeepSign) === 1)
      .drop(Dic.colKeepSign)

    /**
     * 去掉同一用户 同一时间产生相同的订单异常
     * 1.部分数据生效时间差1秒
     * 2.同一时间产生的两个订单，一个支付成功，一个支付不成功，保留支付成功的订单信息
     */

     orderProcessed = orderProcessed.dropDuplicates(Dic.colUserId, Dic.colCreationTime, Dic.colResourceId, Dic.colOrderStatus, Dic.colOrderStartTime)

    //选取同时产生的两个订单中支付成功的(父子订单)
    var orderProcessed2= orderProcessed.groupBy(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime).max(Dic.colOrderStatus)
    orderProcessed2= orderProcessed2.withColumnRenamed("max(order_status)", Dic.colOrderStatus)

    var orderProcessed3 = orderProcessed.join(orderProcessed2, Seq(Dic.colUserId, Dic.colResourceId, Dic.colCreationTime, Dic.colOrderStartTime, Dic.colOrderStatus ), "inner")

    /**
     * 标记金额信息异常用户
     * ！！！！！！！！！！！！！！！！！！！！
     * 目前仅仅用于会员付费预测信息
     */

    orderProcessed = orderProcessed.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceId), col(Dic.colMoney)))

    orderProcessed

  }

}
