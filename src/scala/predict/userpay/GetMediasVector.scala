package predict.userpay

import mam.GetSaveData.{getAllBertVector, getDataFromXXK, saveDataForXXK}
import mam.Utils.{printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.functions._
import train.userpay.GetMediasVector.{assemblerMediasVector, getMediasDigiCatFeature}


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

    val df_medias_scalar = getMediasDigiCatFeature(df_medias_feat)

    // 2-2 Get bert vector data
    val df_medias_bert = getAllBertVector()

    printDf("df_medias_bert", df_medias_bert)

    // 3 word2Vector video id
    // 3 word2Vector vector

    val df_medias_w2v = getDataFromXXK("predict", "predict_videoId_w2v")
    printDf("df_medias_w2v", df_medias_w2v)


    // 4 Bert vector concat word2vector and medias raw digital cat features
    val df_medias_vec = assemblerMediasVector(df_medias_w2v, df_medias_bert, df_medias_scalar)

    // 5 Save processed data
    saveDataForXXK(df_medias_vec, "predict", "predict_medias_bert_w2v_vec")

    printDf("df_medias_vec: PCA De-dimensional concat vector", df_medias_vec)


  }


}
