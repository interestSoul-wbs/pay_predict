package predict.userpay

import mam.GetSaveData.{getPredictUser, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfLpad}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.ml.linalg.Vectors
import train.userpay.User2Vector.{getUsersPartTimePlay, getVideoPlayedUserList, playDays, user2Vec, userVecDimension}

object User2Vector {


  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    val predictTime = args(0) + " " + args(1)
    println("predictTime  ", predictTime)

    // 2 Get Data
    val df_predict_users = getPredictUser(spark, predictTime)
    printDf("输入 df_predict_users", df_predict_users)


    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_play", df_plays)


    // 3  选一段时间的用户播放历史和视频播放用户列表

    val df_play_part = getUsersPartTimePlay(df_plays, df_predict_users, predictTime, playDays)
    printDf("df_play_part", df_play_part)

    val df_video_userList = getVideoPlayedUserList(df_play_part)

    // 4 user Word2Vec
    val df_user2vec = user2Vec(df_video_userList)
    printDf("df_user2vec", df_user2vec)


    val df_predict_userVec = df_predict_users
      .select(Dic.colUserId)
      .join(df_user2vec, Seq(Dic.colUserId), "left")
      .na.fill(Vectors.dense(Vectors.zeros(userVecDimension).toArray).toString)


    printDf("df_predict_userVec", df_predict_userVec)
    saveDataForXXK(df_predict_userVec, "predict", "predict_user_vec")


  }



}
