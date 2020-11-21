package train.common

import mam.Dic
import mam.Utils._
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._
import com.github.nscala_time.time.Imports._

object VideoVectorGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vectorDimension: Int = 64
  var windowSize: Int = 10
  var minCount: Int = 5
  var maxSentenceLength: Int = 30
  var maxIteration: Int = 1
  var date: DateTime = _
  var thirtyDaysAgo: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    thirtyDaysAgo = (date - 30.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-06-01 00:00:00 = args(0) + " " + args(1)
    println("thirtyDaysAgo is : " + thirtyDaysAgo)

    // 1 - get play data.
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    printDf("df_plays", df_plays)

    // 2 - data process
    val df_plays_list = getPlayList(df_plays, thirtyDaysAgo)

    printDf("df_plays_list", df_plays_list)

    println("——————————\n"*4 + "count:  " + df_plays_list.count().toString + "——————————\n"*4)

    // 3 - word2vec training and get vector
    val df_video_dict = getVector(df_plays_list, vectorDimension, windowSize, minCount, maxSentenceLength, maxIteration)

    printDf("df_video_dict", df_video_dict)

    // 4 - save
    saveVideoVector(spark, df_video_dict, partitiondate, license)
  }

}
