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
  var windowSize: Int = 10
  var minCount: Int = 5
  var maxSentenceLength: Int = 30
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

    println("——————————\n"*4, "count:  ", df_plays_list.count(), "——————————\n"*4)

    // 3 - word2vec training and get vector
    val df_video_dict = getVector(df_plays_list, vectorDimension, windowSize, minCount, maxSentenceLength, maxIteration)

    printDf("df_video_dict", df_video_dict)

    // 4 - save
    saveVideoVector(spark, df_video_dict, partitiondate, license)
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
  def getVector(df_plays_list: DataFrame, vector_dimension: Int = 64, window_size: Int = 10, min_count: Int = 5,
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
      .select(
        col(Dic.colVideoId) +: (0 until vectorDimension).map(i => col(Dic.colVector).getItem(i).as(s"v_$i")): _*)

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


  def saveVideoVector(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_video_vector(
        |            video_id string,
        |            v_0 double,
        |            v_1 double,
        |            v_2 double,
        |            v_3 double,
        |            v_4 double,
        |            v_5 double,
        |            v_6 double,
        |            v_7 double,
        |            v_8 double,
        |            v_9 double,
        |            v_10 double,
        |            v_11 double,
        |            v_12 double,
        |            v_13 double,
        |            v_14 double,
        |            v_15 double,
        |            v_16 double,
        |            v_17 double,
        |            v_18 double,
        |            v_19 double,
        |            v_20 double,
        |            v_21 double,
        |            v_22 double,
        |            v_23 double,
        |            v_24 double,
        |            v_25 double,
        |            v_26 double,
        |            v_27 double,
        |            v_28 double,
        |            v_29 double,
        |            v_30 double,
        |            v_31 double,
        |            v_32 double,
        |            v_33 double,
        |            v_34 double,
        |            v_35 double,
        |            v_36 double,
        |            v_37 double,
        |            v_38 double,
        |            v_39 double,
        |            v_40 double,
        |            v_41 double,
        |            v_42 double,
        |            v_43 double,
        |            v_44 double,
        |            v_45 double,
        |            v_46 double,
        |            v_47 double,
        |            v_48 double,
        |            v_49 double,
        |            v_50 double,
        |            v_51 double,
        |            v_52 double,
        |            v_53 double,
        |            v_54 double,
        |            v_55 double,
        |            v_56 double,
        |            v_57 double,
        |            v_58 double,
        |            v_59 double,
        |            v_60 double,
        |            v_61 double,
        |            v_62 double,
        |            v_63 double)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_video_vector
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license')
         |SELECT
         |    video_id,
         |    v_0,
         |    v_1,
         |    v_2,
         |    v_3,
         |    v_4,
         |    v_5,
         |    v_6,
         |    v_7,
         |    v_8,
         |    v_9,
         |    v_10,
         |    v_11,
         |    v_12,
         |    v_13,
         |    v_14,
         |    v_15,
         |    v_16,
         |    v_17,
         |    v_18,
         |    v_19,
         |    v_20,
         |    v_21,
         |    v_22,
         |    v_23,
         |    v_24,
         |    v_25,
         |    v_26,
         |    v_27,
         |    v_28,
         |    v_29,
         |    v_30,
         |    v_31,
         |    v_32,
         |    v_33,
         |    v_34,
         |    v_35,
         |    v_36,
         |    v_37,
         |    v_38,
         |    v_39,
         |    v_40,
         |    v_41,
         |    v_42,
         |    v_43,
         |    v_44,
         |    v_45,
         |    v_46,
         |    v_47,
         |    v_48,
         |    v_49,
         |    v_50,
         |    v_51,
         |    v_52,
         |    v_53,
         |    v_54,
         |    v_55,
         |    v_56,
         |    v_57,
         |    v_58,
         |    v_59,
         |    v_60,
         |    v_61,
         |    v_62,
         |    v_63
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
