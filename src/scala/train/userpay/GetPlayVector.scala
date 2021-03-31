package train.userpay

import mam.GetSaveData.{getDataFromXXK, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.Utils.{calDate, getData, mapIdToMediasVector, printArray, printDf, sysParamSetting, udfBreakList, udfGetTopNHistory, udfLpad}
import org.apache.spark.ml.feature.{VectorAssembler, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lead, lit, row_number, sort_array, udf, when}
import train.userpay.GetMediasForBertAndPlayList.playsNum
import train.userpay.GetMediasVector.pcaDimension

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object GetPlayVector {


  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data

    val df_medias_vector = getDataFromXXK("train", "train_medias_bert_w2v_vec")
    printDf("Input: df_medias_vector", df_medias_vector)

    // 3 Get play history in a past period of time

    val df_train_play = getDataFromXXK("train", "train_play_free_paid_" + playsNum)

    printDf("输出 df_train_play", df_train_play)

    // 4 Map id to vector

    val df_play_vector = mapId2Vec(df_medias_vector, df_train_play)

    val df_play_vec = df_play_vector
      .withColumn(Dic.colPlayFreeVec, col(Dic.colPlayFreeVec).cast("String"))
      .withColumn(Dic.colPlayPaidVec, col(Dic.colPlayPaidVec).cast("String"))

    saveDataForXXK(df_play_vec, "train", "train_play_vector_" + playsNum)

    printDf("df_play_vec", df_play_vec)



  }


  def mapId2Vec2(df_medias_vector: DataFrame, df_train_play: DataFrame) = {


    var df_play_his = df_train_play
    for (i <- 0 to playsNum / 2 - 1) {

      df_play_his = df_play_his
        .withColumnRenamed("free" + i, Dic.colVideoId)
        .join(df_medias_vector, Seq(Dic.colVideoId), "left")
        .withColumnRenamed(Dic.colConcatVec, "free_vec" + i)
        .withColumnRenamed("paid" + i, Dic.colVideoId)
        .join(df_medias_vector, Seq(Dic.colVideoId), "left")
        .withColumnRenamed(Dic.colConcatVec, "paid_vec" + i)
        .drop("free" + i, "paid" + i)


    }

    printDf("df_play_his", df_play_his)


  }


  def mapId2Vec(df_medias_vector: DataFrame, df_play_history: DataFrame) = {

    // Get Medias map( id => Vector)
    import scala.collection.mutable
    val mediasMap = df_medias_vector.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoId).toString -> row.getAs(Dic.colConcatVec).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]


    var df_play_vec = df_play_history

    val freeCols = ArrayBuffer[String]()
    val paidCols = ArrayBuffer[String]()

    for (i <- 0 to playsNum / 2 - 1) {

      // Map free play list
      df_play_vec = df_play_vec
        .withColumn("free_vec" + i, udfStr2Vec(mapId2MediasVector(mediasMap)(col("free" + i))))
        .withColumn("paid_vec" + i, udfStr2Vec(mapId2MediasVector(mediasMap)(col("paid" + i))))
        .drop("free" + i)
        .drop("paid" + i)

      freeCols.append("free_vec" + i)
      paidCols.append("paid_vec" + i)


    }

    printDf("df_play_vec", df_play_vec)


    // concat vec_i to vector
    val assembler1 = new VectorAssembler()
      .setInputCols(freeCols.toArray)
      .setOutputCol(Dic.colPlayFreeVec)

    val df_free = assembler1.transform(df_play_vec)
    printDf("df_free", df_free)

    val assembler2 = new VectorAssembler()
      .setInputCols(paidCols.toArray)
      .setOutputCol(Dic.colPlayPaidVec)

    val df_vec = assembler2.transform(df_free)
    printDf("df_vec", df_vec)

    df_vec.select(Dic.colUserId, Dic.colPlayFreeVec, Dic.colPlayPaidVec)




  }

  def mapId2MediasVector(mediaMap: mutable.HashMap[String, String]) = udf((videoId: String) =>

        mediaMap.getOrElse(videoId, Vectors.zeros(pcaDimension).toString)
  )


  def udfStr2Vec = udf(str2Vec _)

  def str2Vec(str: String) = {

    var vecArr = ArrayBuffer[Double]()
    str.substring(1, str.length - 1).split(",").foreach(item =>

      vecArr.append(item.toDouble)
    )

    Vectors.dense(vecArr.toArray)

  }


}
