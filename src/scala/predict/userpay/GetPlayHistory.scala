package predict.userpay

import mam.GetSaveData.{getPredictUser, getProcessedMedias, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, functions}

object GetPlayHistory {

  val playsNum = 40

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
    printDf("df_predict_users", df_predict_users)

    val df_predict_ids = df_predict_users.select(Dic.colUserId)

    // 3 Get play history in a past period of time

    getPlayHistory(df_plays, predictTime, df_predict_ids, 40, df_medias)

  }

  def getPlayHistory(df_play: DataFrame, predictTime: String, df_predict_ids : DataFrame, playsNum: Int, df_medias:DataFrame) = {

    val df_plays_train = df_play.filter(
      col(Dic.colPlayStartTime) < (predictTime)
      && col(Dic.colPlayStartTime) >= calDate(predictTime, -60)
    )

    val df_predict_plays = df_plays_train.join(df_predict_ids, Seq(Dic.colUserId), "inner")

    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)

    // Latest 40 play histories
    val df_predict_select = df_predict_plays
      .withColumn("user_play_count",
        functions.count(Dic.colUserId)
          .over(win1)
      ).filter(
      col("user_play_count") .< (playsNum)
    )
      .drop("user_play_count")


    printDf("df_predict_select", df_predict_select)


    saveDataForXXK(df_predict_select, "predict", "predict_play_history_40")

    // Medias in those play histories

    val df_video_ids = df_predict_select.select(Dic.colVideoId).dropDuplicates()

    val df_medias_part = df_medias.join(df_video_ids, Seq(Dic.colVideoId), "inner")

    printDf("df_medias_part", df_medias_part)

    saveDataForXXK(df_medias_part, "predict", "medias_in_play_history_40_predict")



  }



}
