package train.userpay

import mam.GetSaveData.{getDataFromXXK, saveDataForXXK}
import mam.Utils.{printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import train.userpay.GetMediasForBertAndPlayList.playsNum

object GetAllPlayedMediasText {


  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data

    val df_train_text = getDataFromXXK("train", "train_medias_text_" + playsNum)
    printDf("df_train_text", df_train_text)

    val df_predict_text = getDataFromXXK("predict", "predict_medias_text_" + playsNum)
    printDf("df_predict_text", df_predict_text)


    val df_train_predict_text = df_train_text.union(df_predict_text).dropDuplicates(Dic.colVideoId)
    printDf("df_train_predict_text", df_train_predict_text)


    saveDataForXXK(df_train_predict_text, "common", "train_predict_text")


  }

}
