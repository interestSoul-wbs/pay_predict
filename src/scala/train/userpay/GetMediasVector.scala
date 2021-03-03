package train.userpay

import mam.Dic.colVector
import mam.GetSaveData.{getBertVector, hdfsPath, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.ml.feature.{PCA, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, from_json, lit, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GetMediasVector {

  // word2vector参数
  val vectorDimension = 256 // 向量维度
  val windowSize = 10 //滑动窗口大小， 默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
  val pcaDimension = 64  // 向量维度


  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()


    // 2 Get data
    // Get bert vector data
    val df_medias_bert_raw = getBertVector("train")
    printDf("输入 df_medias_bert_raw", df_medias_bert_raw)

    val df_medias_bert = df_medias_bert_raw
          .select(
            when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
            from_json(col(Dic.colBertVector), ArrayType(StringType, containsNull = true)).as(Dic.colBertVector)

          )

    printDf("df_medias_bert", df_medias_bert)

    // 3 word2Vector video id
    // 对videoId进行word2vec
    val df_medias_id = df_medias_bert.agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoId + "_list"))

    printDf("df_medias_id", df_medias_id)

    val df_medias_w2v = w2vec(df_medias_id)
    printDf("df_medias_w2v", df_medias_w2v)

    val df_medias_ver = df_medias_w2v.withColumn(colVector, col(Dic.colVector).cast(StringType))

    // Bert vector concat word2vector

    val df_medias_vec = df_medias_bert.join(df_medias_ver, Seq(Dic.colVideoId), "left")

    printDf("df_medias_vec", df_medias_vec)

    val df_medias_concat = df_medias_vec.withColumn(Dic.colConcatVec, udfConcatVector(col(Dic.colBertVector), col(Dic.colVector)))

    printDf("df_medias_concat: concat bert and w2v", df_medias_concat)


    //4 PCA De-dimensional

    val df_medias_pca = GetPCA(df_medias_concat)



    // For nan play history


    val df_fill = spark.createDataFrame(
      Seq(("0", Vectors.dense(Vectors.zeros(pcaDimension).toArray)))
    ).toDF(Dic.colVideoId, Dic.colConcatVec)

    val df_medias_pca_all = df_fill.union(df_medias_pca)

    // 5 Save processed data
    saveDataForXXK(df_medias_pca_all, "train", "train_medias_bert_w2v")

    printDf("df_medias_pca_all: PCA De-dimensional concat vector", df_medias_pca_all)


  }

  // word2vec
  def w2vec(df_medias:DataFrame) ={

    val w2vModel = new Word2Vec()
      .setInputCol(Dic.colVideoId + "_list")
      .setOutputCol(Dic.colVector)
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(0)
    val model = w2vModel.fit(df_medias)

    model.getVectors
      .withColumnRenamed("word", Dic.colVideoId)
  }


  // concat Bert vector and word2Vec
  def udfConcatVector = udf(concatVector _)

  def concatVector(bert_vector: mutable.WrappedArray[String], w2vector: String) = {

    var vectorArray = new ArrayBuffer[Double]()

    bert_vector.foreach(str =>
      vectorArray.append(str.toDouble)
    )

    w2vector.substring(1, w2vector.length - 1).split(",").foreach(
    item =>
      vectorArray.append(item.toDouble)

    )

    val v = Vectors.dense(vectorArray.toArray)
    v

  }



  def GetPCA(df: DataFrame) = {
    val pca = new PCA()
      .setInputCol(Dic.colConcatVec)
      .setOutputCol(Dic.colConcatVec + "PCA")
      .setK(pcaDimension)
      .fit(df)


    pca.transform(df)
      .select(Dic.colVideoId, Dic.colConcatVec + "PCA")
      .withColumnRenamed(Dic.colConcatVec + "PCA", Dic.colConcatVec)

  }

}
