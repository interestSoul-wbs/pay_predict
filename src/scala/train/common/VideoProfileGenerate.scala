package train.common

import java.text.SimpleDateFormat

import mam.Utils.calDate
import mam.Utils.udfGetDays
import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

object VideoProfileGenerate {

  def videoProfileGenerate(now:String,timeWindow:Int,medias_path:String,plays_path:String,orders_path:String): Unit ={
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("VideoProfileGenerate")
      //.master("local[6]")
      .getOrCreate()
    //设置shuffle过程中分区数
    // spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1000")
    import org.apache.spark.sql.functions._

    val medias = spark.read.format("parquet").load(medias_path)
    val plays = spark.read.format("parquet").load(plays_path)
    val orders = spark.read.format("parquet").load(orders_path)


    val pre_30 = calDate(now, -30)
    val pre_14 = calDate(now, days = -14)
    val pre_7 = calDate(now, -7)
    val pre_3 = calDate(now, -3)
    val pre_1 = calDate(now, -1)
    val joinKeysVideoId = Seq(Dic.colVideoId)


    val part_11=plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_30)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn30Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin30Days)
      )
    val part_12=plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_14)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn14Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin14Days)
      )
    val part_13=plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_7)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn7Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin7Days)
      )
    val part_14=plays
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colPlayEndTime).>=(pre_3)
      )
      .groupBy(col(Dic.colVideoId))
      .agg(
        count(col(Dic.colVideoId)).as(Dic.colNumberOfPlaysIn3Days),
        countDistinct(col(Dic.colUserId)).as(Dic.colNumberOfViewsWithin3Days)
      )

    val part_15=medias
      .withColumn(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent,udfGetDays(col(Dic.colStorageTime),lit(now)))
      .select(col(Dic.colVideoId),col(Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))



    val result1=medias.join(part_11,joinKeysVideoId,"left")
    val result2=result1.join(part_12,joinKeysVideoId,"left")
    val result3=result2.join(part_13,joinKeysVideoId,"left")
    val result4=result3.join(part_14,joinKeysVideoId,"left")
    val result5=result4.join(part_15,joinKeysVideoId,"left")



    val part_21=orders.filter(
      col(Dic.colCreationTime).<(now)
      && col(Dic.colCreationTime).>=(pre_30)
      && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
          count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin30Days)
      )
    val part_22=orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_14)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin14Days)
      )
    val part_23=orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_7)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin7Days)
      )
    val part_24=orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colCreationTime).>=(pre_3)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedWithin3Days)
      )
    val part_25=orders.filter(
      col(Dic.colCreationTime).<(now)
        && col(Dic.colOrderStatus).>(1)
    )
      .groupBy(col(Dic.colResourceId))
      .agg(
        count(col(Dic.colResourceId)).as(Dic.colNumberOfTimesPurchasedTotal)
      )
    val result6=result5.join(part_21,result5.col(Dic.colVideoId)===part_21.col(Dic.colResourceId),"left")
      .select(result5.col("*"),part_21.col(Dic.colNumberOfTimesPurchasedWithin30Days))
    val result7=result6.join(part_22,result6.col(Dic.colVideoId)===part_22.col(Dic.colResourceId),"left")
      .select(result6.col("*"),part_22.col(Dic.colNumberOfTimesPurchasedWithin14Days))
    val result8=result7.join(part_23,result7.col(Dic.colVideoId)===part_23.col(Dic.colResourceId),"left")
      .select(result7.col("*"),part_23.col(Dic.colNumberOfTimesPurchasedWithin7Days))
    val result9=result8.join(part_24,result8.col(Dic.colVideoId)===part_24.col(Dic.colResourceId),"left")
      .select(result8.col("*"),part_24.col(Dic.colNumberOfTimesPurchasedWithin3Days))
    val result10=result9.join(part_25,result9.col(Dic.colVideoId)===part_25.col(Dic.colResourceId),"left")
      .select(result9.col("*"),part_25.col(Dic.colNumberOfTimesPurchasedTotal))

    //result10.show()
    //选出数据类型为数值类型的列
    val numColumns=new ListBuffer[String]
    for(elem<-result10.dtypes){
      if(elem._2.equals("DoubleType")||elem._2.equals("LongType")||elem._2.equals("IntegerType")){
          numColumns.insert(numColumns.length,elem._1)
      }
    }

    //将其他类型的列转化为字符串，容易保存为csv文件
    val anoColumns=result10.columns.diff(numColumns)
    val result11= anoColumns.foldLeft(result10){
      (currentDF, column) => currentDF.withColumn(column, col(column).cast("string"))
    }
    result10.na.fill(0,numColumns)
    result11.na.fill(0,numColumns)

   // result10.show()
    //result11.show()

    val videoProfilePath="hdfs:///pay_predict/data/train/common/processed/videoprofile"+now.split(" ")(0)
   // val videoProfileSavePath="pay_predict/data/train/common/processed/videoprofile.csv"
    result10.write.mode(SaveMode.Overwrite).format("parquet").save(videoProfilePath)
    //result11.write.mode(SaveMode.Overwrite).format("parquet").save(userProfileOrderPartSavePath)


  }

  def main(args:Array[String]): Unit = {
    val mediasProcessedPath="hdfs:///pay_predict/data/train/common/processed/mediastemp"
    val playsProcessedPath="hdfs:///pay_predict/data/train/common/processed/plays"
    val ordersProcessedPath="hdfs:///pay_predict/data/train/common/processed/orders"
    val now=args(0)+" "+args(1)
    videoProfileGenerate(now,30,mediasProcessedPath,playsProcessedPath,ordersProcessedPath)
  }

}
