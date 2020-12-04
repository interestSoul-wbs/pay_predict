package train.common

import mam.Dic
import mam.Utils._
import mam.GetSaveData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object PlaysProcess {

  var timeMaxLimit = 43200
  var timeMinLimit = 30

  def main(args: Array[String]): Unit = {

    sysParamSetting()

    val spark = SparkSession
      .builder()
      //.master("local[6]") // Konverse - 上传 master 时，删除
//      .enableHiveSupport() // Konverse - 这个如果不影响本地运行，就不用注释；
      .getOrCreate()

    val df_play_raw = getRawPlays(spark)
    printDf("df_play_raw", df_play_raw)

    val df_play_processed = playsProcess(df_play_raw, timeMaxLimit, timeMinLimit)
    printDf("df_play_processed", df_play_processed)

    saveProcessedPlay(df_play_processed)

    println("播放数据处理完成！")
  }

  def playsProcess(df_play_raw: DataFrame, timeMaxLimit: Int, timeMinLimit: Int) = {

    val df_play_processed = df_play_raw
      .withColumn(Dic.colPlayEndTime, substring(col(Dic.colPlayEndTime), 0, 10))
      .groupBy(col(Dic.colUserId), col(Dic.colVideoId), col(Dic.colPlayEndTime))
      .agg(
        sum(col(Dic.colBroadcastTime)) as Dic.colBroadcastTime)
      .filter(col(Dic.colBroadcastTime) < timeMaxLimit && col(Dic.colBroadcastTime) > timeMinLimit)
      .orderBy(col(Dic.colUserId), col(Dic.colPlayEndTime))
      .withColumn(Dic.colPlayEndTime, udfAddSuffix(col(Dic.colPlayEndTime)))

    df_play_processed
  }
}



