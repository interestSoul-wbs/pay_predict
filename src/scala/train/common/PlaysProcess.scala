package train.common

import mam.Dic
import mam.Utils.{printDf, udfAddSuffix}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object PlaysProcess {

  val time_max_limit=43200
  val time_min_limit=30

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      //.master("local[4]")
      .appName("PlaysProcess")
      .getOrCreate()

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val playRawPath=hdfsPath+"data/train/common/raw/plays/behavior_*.txt"
    val playProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val playRaw=getRawPlays(playRawPath,spark)

    printDf("输入 playRaw",playRaw)

    val playsProcessed=playsProcess(playRaw)


    printDf("输出 playProcessed",playsProcessed)
    playsProcessed.write.mode(SaveMode.Overwrite).format("parquet").save(playProcessedPath)
     println("播放数据处理完成！")
  }
  def getRawPlays(playRawPath:String,spark:SparkSession)={
    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)
      )
    )
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(playRawPath)
    df

  }
  def playsProcess(df_raw_play:DataFrame)={

    val df_play_processed = df_raw_play
      .withColumn(Dic.colPlayEndTime,substring(col(Dic.colPlayEndTime),0,10))
      .groupBy(col(Dic.colUserId),col(Dic.colVideoId),col(Dic.colPlayEndTime))
      .agg(
        sum(col(Dic.colBroadcastTime)) as Dic.colBroadcastTime)
      .filter(col(Dic.colBroadcastTime)<time_max_limit && col(Dic.colBroadcastTime)>time_min_limit )
      .orderBy(col(Dic.colUserId),col(Dic.colPlayEndTime))
      .withColumn(Dic.colPlayEndTime,udfAddSuffix(col(Dic.colPlayEndTime)))

    df_play_processed
  }

}



