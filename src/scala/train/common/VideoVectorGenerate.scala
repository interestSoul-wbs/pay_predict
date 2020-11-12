package train.common

import mam.Dic
import mam.Utils.printDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, collect_list, lit, substring, udf, _}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object VideoVectorGenerate {

  var vectorDimension: Int = 64
  var windowSize: Int = 10

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils")

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("VideoVectorGenerate")
      //.master("local[6]")
      .getOrCreate()

    val hdfsPath = "hdfs:///pay_predict/"

    val playsProcessedPath = hdfsPath + "data/train/common/processed/plays"

    val now = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark, playsProcessedPath)

    //构建视频列表
    val df_plays_list = getPlayList(df_plays, now)

    //用户每月平均观看视频32个
    val df_video_dict = getVector(df_plays_list, vectorDimension, windowSize)

    printDf("df_video_dict", df_video_dict)

    val videoVectorPath = hdfsPath + "data/train/common/processed/videovector" + args(0)
    df_video_dict.write.mode(SaveMode.Overwrite).format("parquet").save(videoVectorPath)
  }

  def getProcessedPlay(spark: SparkSession, playsProcessedPath: String) = {
    spark.read.format("parquet").load(playsProcessedPath)
  }

  def getPlayList(df_plays: DataFrame, now: String) = {

    val df_play_list = df_plays
      .withColumn(Dic.colPlayMonth, substring(col(Dic.colPlayEndTime), 0, 7))
      .filter(col(Dic.colPlayEndTime).<(lit(now)))
      .groupBy(col(Dic.colUserId), col(Dic.colPlayMonth))
      .agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoList))

    df_play_list
  }

  def getVector(df_plays_list: DataFrame, vector_dimension: Int = 64, window_size: Int = 5,input_col: String = Dic.colVideoList,
                output_col: String = Dic.colResult) = {

    val w2vModel = new Word2Vec()
      .setInputCol(input_col)
      .setOutputCol(output_col)
      .setVectorSize(vector_dimension)
      .setWindowSize(window_size)
      .setMinCount(5)

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
    * @return
    */
  def udfVectorToArray = udf(vectorToArray _)

  def vectorToArray(col_vector: Vector) = {
    col_vector.toArray
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
}
