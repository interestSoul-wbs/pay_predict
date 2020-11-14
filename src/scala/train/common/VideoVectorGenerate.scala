package train.common

import mam.Dic
import mam.Utils.printDf
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.GetSaveData._

object VideoVectorGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vectorDimension: Int = 64
  var windowSize: Int = 5
  var minCount: Int = 5
  var maxSentence: Int = 30
  var maxIteration: Int = 1

  // Konverse - 2020-11-3 - 还没跑通 - df_raw_video_dict 正常
  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 2020-06-01 00:00:00 = args(0) + " " + args(1)
    val now = "2020-09-01 00:00:00"

    // 1 - get play data.
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    printDf("df_plays", df_plays)

    // 2 - data process
    val df_plays_list = getPlayList(df_plays, now)

    printDf("df_plays_list", df_plays_list)

    // 3 - word2vec training and get vector
    val df_video_dict = getVector(df_plays_list, vectorDimension, windowSize, minCount, maxSentence)

    printDf("df_video_dict", df_video_dict)

    // 4 - save
    // 2020-11-12 - 先打出来看看，然后再看怎么存
  }


  /**
    * Transform Spark vectors to double.
    *
    * @return
    */
  def udfBreak = udf(break _)

  def break(array: Object, index: Int) = {

    val vectorString = array.toString

    vectorString.substring(1, vectorString.length - 1).split(",")(index).toDouble
  }

  /**
    * Word2vec training.
    */
  def getVector(df_plays_list: DataFrame, vector_dimension: Int = 64, window_size: Int = 5, min_count: Int = 5,
                max_length: Int = 30, max_iteration: Int = 1,
                input_col: String = Dic.colVideoList, output_col: String = Dic.colResult) = {

    val w2vModel = new Word2Vec()
      .setInputCol(input_col)
      .setOutputCol(output_col)
      .setVectorSize(vector_dimension)
      .setWindowSize(window_size)
      .setMinCount(min_count)
      .setMaxSentenceLength(max_length)
      .setMaxIter(max_iteration)

    val model = w2vModel.fit(df_plays_list)

    val df_raw_video_dict = model
      .getVectors
      .select(
        col(Dic.colWord).as(Dic.colVideoId),
        udfVectorToArray(
          col(Dic.colVector)).as(Dic.colVector))

    val df_video_dict = splitVecToDouble(df_raw_video_dict)

    df_video_dict
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

  def getPlayList(df_plays: DataFrame, now: String) = {

    val df_play_list = df_plays
      .withColumn(Dic.colPlayMonth, substring(col(Dic.colPlayEndTime), 0, 7))
      .filter(col(Dic.colPlayEndTime).<(lit(now)))
      .groupBy(col(Dic.colUserId), col(Dic.colPlayMonth))
      .agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoList))

    df_play_list
  }
}
