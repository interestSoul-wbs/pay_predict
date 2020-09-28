package train.common

import java.text.SimpleDateFormat

import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

object OrdersProcess {

  def udfChangeDateFormat=udf(changeDateFormat _)
  def changeDateFormat(date:String)= {
    if(date=="NULL"){
      "NULL"
    }else{
      //
      try{
        val sdf=new SimpleDateFormat("yyyyMMddHHmmSS")
        val dt:Long=sdf.parse(date).getTime()
        val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt)
        new_time
      }
      catch{
        case e: Exception =>{
          "NULL"
        }
      }
    }

  }


    def main(args: Array[String]): Unit ={
      System.setProperty("hadoop.home.dir","c:\\winutils")
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark: SparkSession = new sql.SparkSession.Builder()
        .appName("OrdersProcess")
        //.master("local[6]")
        .getOrCreate()
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

      import org.apache.spark.sql.functions._
      val orderRawPath="hdfs:///pay_predict/data/train/common/raw/orders/order*.txt"
      val orderProcessedPath="hdfs:///pay_predict/data/train/common/processed/orders"
      val df = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .schema(schema)
        .csv(orderRawPath)

      val df1=df.withColumn(Dic.colCreationTime,udfChangeDateFormat(col(Dic.colCreationTime)))
        .withColumn(Dic.colOrderStartTime,udfChangeDateFormat(col(Dic.colOrderStartTime)))
        .withColumn(Dic.colOrderEndTime,udfChangeDateFormat(col(Dic.colOrderEndTime)))
      
      val df2=df1.select(
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
      //df2.show()
      df2.write.mode(SaveMode.Overwrite).format("parquet").save(orderProcessedPath)
      println("订单数据处理完成！")
    }

  }
