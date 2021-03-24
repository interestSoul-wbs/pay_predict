package predict.userpay

import mam.Dic.colVector
import mam.GetSaveData.{getBertVector, getDataFromXXK, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}
import train.userpay.GetMediasVector.{GetPCA, pcaDimension, udfArrayToVec, w2vec}


object GetMediasVector {

  val predictPredictTimeGap = 15

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()


    // 2 Get data
    // 2-1 获得媒资的数值型类别型特征

    val df_medias_digi = getDataFromXXK("common", "medias_digital_category_feature")

    val df_medias_feat = df_medias_digi.withColumn(Dic.colStorageTimeGap, col(Dic.colStorageTimeGap) + predictPredictTimeGap)
    printDf("输入 df_medias_feat", df_medias_feat)


    // 数值和类别型特征进行组合
    val assembler = new VectorAssembler()
      .setInputCols(df_medias_feat.columns.drop(1)) // drop video id
      .setOutputCol(Dic.colDigitalCategoryVec)

    val df_medias_feature = assembler.transform(df_medias_feat)
      .select(Dic.colVideoId, Dic.colDigitalCategoryVec)

    printDf("df_medias_feature", df_medias_feature)


    // 标准化
    val scaler = new StandardScaler()
      .setInputCol(Dic.colDigitalCategoryVec)
      .setOutputCol(Dic.colDigitalCategoryScalaVec)
      .setWithStd(true) //将数据标准化到单位标准差。
      .setWithMean(true) //是否变换为0均值。
    val df_medias_scalar = scaler
      .fit(df_medias_feature)
      .transform(df_medias_feature)
      .select(Dic.colVideoId, Dic.colDigitalCategoryScalaVec)

    printDf("df_medias_scalar", df_medias_scalar)


    // 2-2 Get bert vector data
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


    // 4 Bert vector concat word2vector and medias raw digital cat features

    val df_medias_vec = df_medias_bert
      .join(df_medias_w2v, Seq(Dic.colVideoId), "left")
      .join(df_medias_scalar, Seq(Dic.colVideoId), "left")
      .withColumn(Dic.colBertVector, udfArrayToVec(col(Dic.colBertVector)))


    printDf("df_medias_vec", df_medias_vec)


    val assemblerVideo = new VectorAssembler()
      .setInputCols(Array(Dic.colBertVector, Dic.colVector, Dic.colDigitalCategoryScalaVec))
      .setOutputCol(Dic.colConcatVec)
      .setHandleInvalid("skip")  // Null

    val df_medias_concat = assemblerVideo.transform(df_medias_vec)
      .select(Dic.colVideoId, Dic.colConcatVec)

    printDf("df_medias_concat", df_medias_concat)


    //4 PCA De-dimensional

    val df_medias_pca = GetPCA(df_medias_concat)

    // For nan play history

    val df_fill = spark.createDataFrame(
      Seq(("0", Vectors.dense(Vectors.zeros(pcaDimension).toArray)))
    ).toDF(Dic.colVideoId, Dic.colConcatVec)

    val df_medias_pca_all = df_fill.union(df_medias_pca)

    // 5 Save processed data
    saveDataForXXK(df_medias_pca_all, "predict", "predict_medias_bert_w2v_vec")

    printDf("df_medias_pca_all: PCA De-dimensional concat vector", df_medias_pca_all)



  }



}
