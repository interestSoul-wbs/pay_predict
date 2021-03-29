package predict.userpay

import mam.GetSaveData.{getPredictUser, getProcessedMedias, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils._
import mam.{Dic, SparkSessionInit}
import train.userpay.GetMediasForBertAndPlayList.{days, getPartMediasForBert, getPlayW2V, getUserFreePaidPlayList, getUserPartTimeTopNPlay, playsNum, udfRemovePunctuation}

object GetMediasForBertAndPlayList {

  val days = 60 // 当前时间前N天的播放记录

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val predictTime = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)

    val df_predict_users = getPredictUser(spark, predictTime)
    printDf("输入 df_predict_users", df_predict_users)

    val df_predict_ids = df_predict_users.select(Dic.colUserId)
    val df_predict_select = getUserPartTimeTopNPlay(df_plays, df_predict_ids, predictTime, days)


    val df_videoId2Vec = getPlayW2V(df_predict_select)

    saveDataForXXK(df_videoId2Vec, "predict", "predict_videoId_w2v" )


//    // Medias text info Bert
//    val df_medias_part = getPartMediasForBert(df_predict_select, df_medias)
//    saveDataForXXK(df_medias_part, "predict", "predict_medias_text_" + playsNum )
//
//    //Users' play list
//    val df_play_list = getUserFreePaidPlayList(df_predict_select, df_medias_part, df_predict_ids)
//    saveDataForXXK(df_play_list, "predict", "predict_play_free_paid_" + playsNum )



  }









}
