package predict.common

import mam.Dic
import mam.Utils.printDf
import org.apache.avro.SchemaBuilder.array
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector

object VideoVectorGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vectorDimension: Int = 64

  // Konverse - 2020-11-3 - 还没跑通 - df_raw_video_dict 正常
  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-06-01 00:00:00 = args(0) + " " + args(1)
    val now = "2020-07-01 00:00:00"

    // 1 - get play data.
    val df_plays = getPlay(spark)

    printDf("df_plays", df_plays)

    // 2 - data process
    val df_plays_list = df_plays
      .filter(col(Dic.colPlayEndTime).<(lit(now)))
      .groupBy(col(Dic.colUserId))
      .agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoList))

    printDf("df_plays_list", df_plays_list)

    // 3 - word2vec training
    val df_raw_video_dict = word2vecTraining(df_plays_list)

    printDf("df_raw_video_dict", df_raw_video_dict)

    // 4 - convert vector data to single column
    val df_video_dict = splitVecToDouble(df_raw_video_dict)

    printDf("df_video_dict", df_video_dict)
  }


  /**
    * Get user play data.
    *
    * @param spark
    * @return
    */
  def getPlay(spark: SparkSession) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    vodrs.t_sdu_user_play_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }

  /**
    * Transform Spark vectors to double.
    * @return
    */
  def udfBreak = udf(break _)

  def break(array:Object,index:Int) ={

    val vectorString=array.toString

    vectorString.substring(1,vectorString.length-1).split(",")(index).toDouble
  }

  /**
    * Word2vec training.
    * @param df_plays_list
    * @param vectorDimension
    * @param input_col
    * @param output_col
    * @return
    */
  def word2vecTraining(df_plays_list: DataFrame, vectorDimension: Int = 64, input_col: String = "video_list",
                       output_col: String = "result") = {

    val w2vModel = new Word2Vec()
      .setInputCol(input_col)
      .setOutputCol(output_col)
      .setVectorSize(vectorDimension)
      .setMinCount(5)

    val model = w2vModel.fit(df_plays_list)

    val df_raw_video_dict = model
      .getVectors
      .select(
        col(Dic.colWord).as(Dic.colVideoId),
        udfVectorToArray(
          col(Dic.colVector)).as(Dic.colVector))

    df_raw_video_dict
  }

  /**
    *
    * @param df_raw_video_dict
    * @return
    */
  def splitVecToDouble(df_raw_video_dict: DataFrame) = {

    val df_video_dict = df_raw_video_dict
      .withColumn("temp", split(col(Dic.colVector), ","))
      .select(
        col(Dic.colVideoId) +: (0 until vectorDimension).map(i => col("temp").getItem(i).as(s"v_$i")): _*)

    df_video_dict
  }

  /**
    *
    * @return
    */
  def udfVectorToArray = udf(vectorToArray _)

  def vectorToArray(col_vector: Vector) = {
    col_vector.toArray
  }
}
