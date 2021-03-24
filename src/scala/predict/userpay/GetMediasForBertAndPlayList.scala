package predict.userpay

import mam.GetSaveData.{getPredictUser, getProcessedMedias, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.SparkSessionInit.spark
import mam.Utils._
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import train.userpay.GetMediasForBertAndPlayList.{days, playsNum, udfRemovePunctuation}

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
    printDf("df_predict_users", df_predict_users)

    val df_predict_ids = df_predict_users.select(Dic.colUserId)


    val df_play_list = getMediasForBertAndPlayList(df_plays, df_medias, df_predict_ids, predictTime)
    saveDataForXXK(df_play_list, "predict", "predict_play_free_paid_" + playsNum)


  }

  def getMediasForBertAndPlayList(df_play: DataFrame, df_medias: DataFrame, df_train_ids: DataFrame, trainTime: String) = {

    // 一段时间的播放历史
    val df_plays_part = df_play.filter(
      col(Dic.colPlayStartTime) < (trainTime)
        && col(Dic.colPlayStartTime) >= calDate(trainTime, -days)
    )

    // 训练集用户的播放历史
    val df_train_plays = df_plays_part.join(df_train_ids, Seq(Dic.colUserId), "inner")

    // 对每个用户选取前N条播放历史
    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)

    val df_train_select = df_train_plays
      .withColumn(Dic.colUserPlayCount,
        row_number.over(win1)
      ).filter(
      col(Dic.colUserPlayCount).<(playsNum)
    )


    printDf("df_predict_select", df_train_select)



    /**
     * 相应播放历史的媒资数据处理
     * 处理之后的brief进行bert
     */

    val df_video_ids = df_train_select.select(Dic.colVideoId).dropDuplicates()

    val df_medias_part = df_medias.join(df_video_ids, Seq(Dic.colVideoId), "inner")
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId) > 0, 1).otherwise(0))
      .na.fill(
      Map((Dic.colVideoTitle, " "), (Dic.colIntroduction, " "))
    )
      .withColumn(
        Dic.colBrief,  udfRemovePunctuation(col(Dic.colVideoTitle), col(Dic.colIntroduction))
      )
      .select(Dic.colVideoId, Dic.colInPackage, Dic.colBrief)

    printDf("df_medias_part", df_medias_part)


    // Save Medias for Bert
    saveDataForXXK(df_medias_part, "predict", "predict_medias_" + playsNum)


    val df_play_history = df_train_select
      .join(df_medias_part, Seq(Dic.colVideoId), "left")

    val df_free_play = df_play_history.filter(col(Dic.colInPackage) === 0)
    val df_paid_play = df_play_history.filter(col(Dic.colInPackage) === 1)

    val df_free_col = getUsersPlayList(df_free_play, "free", playsNum / 2)
    val df_paid_col = getUsersPlayList(df_paid_play, "paid", playsNum / 2)


    val df_play_list = df_train_ids
      .join(df_free_col, Seq(Dic.colUserId), "left")
      .join(df_paid_col, Seq(Dic.colUserId), "left")
      .na.fill("0")


    df_play_list
  }



  // 去除medais的brief列中的标点符号
  def udfRemovePunctuation = udf(removePunctuation _)

  def removePunctuation(videoTitle: String, introduction: String) = {
    val regex = "[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]".r
    val brief = videoTitle + introduction
    val new_brief = regex.replaceAllIn(brief, "")

    if (new_brief.length < 126)
      new_brief
    else
      new_brief.substring(0, 126)


  }




  def getUsersPlayList(df_play: DataFrame, colName: String, topNPlay: Int) = {

    //获取数字位数
    val rowCount = df_play.count().toString.length
    println("df count number length", rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime).desc)

    /**
     * This part is to ensure the sequence has correct order after spark cluster
     * The order is desc(play_start_time)
     */
    val df_play_temp = df_play.withColumn(Dic.colUserPlayCount, row_number().over(win))
      //为播放的视频id进行排序
      .withColumn("0", lit("0"))
      .withColumn("tmp_rank", udfLpad(col(Dic.colUserPlayCount), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(Dic.colVideoId)))
      //将list中的video按照播放次序排列
      .groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(colName + "_list", udfGetTopNHistory(col("tmp_column_1"), lit(topNPlay)))
      .select(Dic.colUserId, colName + "_list")

    printDf("df_play_temp", df_play_temp)


    // 拆分播放列表
    var df_play_data = df_play_temp

    for(i <- 0 to topNPlay - 1)
      df_play_data = df_play_data.withColumn(colName + i,udfBreakList(col(colName + "_list"), lit(i)))

    printDf("df_play_data", df_play_data)

    df_play_data.drop(colName + "_list")

  }









}
