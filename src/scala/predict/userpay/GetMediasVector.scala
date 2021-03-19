package predict.userpay

import mam.Dic.colVector
import mam.GetSaveData.{getBertVector, getDataFromXXK, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.ml.feature.{PCA, VectorAssembler, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import train.userpay.GetMediasVector.{GetPCA, pcaDimension, udfConcatVector, vectorDimension, w2vec, windowSize}


object GetMediasVector {

  val trainPredictTimeGap = 15

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()


    // 2 Get data
    // 2-1 获得媒资的数值型类别型特征

    val df_medias_feat = getDataFromXXK("common", "medias_digital_category_feature")

    val df_medias_feature = df_medias_feat.withColumn(Dic.colStorageTimeGap, col(Dic.colStorageTime) + trainPredictTimeGap)
    printDf("输入 df_medias_feature", df_medias_feature)

    // 数值和类别型特征进行组合
    val assembler = new VectorAssembler()
      .setInputCols(df_medias_feature.columns.drop(1)) // drop video id
      .setOutputCol(Dic.colDigitalCategoryVec)

    val df_medias_vector = assembler.transform(df_medias_feature)
      .select(Dic.colVideoId, Dic.colDigitalCategoryVec)
      .withColumn(Dic.colDigitalCategoryVec, col(Dic.colDigitalCategoryVec).cast(StringType))

    printDf("df_medias_vector", df_medias_vector)





    // Get bert vector data
    val df_medias_bert_raw = getBertVector("predict")
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

    val df_medias_concat = df_medias_vec.withColumn(Dic.colConcatVec,
      udfConcatVector(col(Dic.colBertVector), col(Dic.colVector), col(Dic.colDigitalCategoryVec)))

    printDf("df_medias_concat: concat bert and w2v", df_medias_concat)



    //4 PCA De-dimensional

    val df_medias_pca = GetPCA(df_medias_concat)



    // For nan play history


    val df_fill = spark.createDataFrame(
      Seq(("0", Vectors.dense(Vectors.zeros(pcaDimension).toArray)))
    ).toDF(Dic.colVideoId, Dic.colConcatVec)

    val df_medias_pca_all = df_fill.union(df_medias_pca)

    // 5 Save processed data
    saveDataForXXK(df_medias_pca_all, "predict", "predict_medias_bert_w2v")

    printDf("df_medias_pca_all: PCA De-dimensional concat vector", df_medias_pca_all)


  }



}
