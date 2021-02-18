package train.userpay

import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printArray, printDf, sysParamSetting, udfBreakList, udfGetTopNHistory, udfLpad}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lead, lit, row_number, sort_array, udf, when}

object GetPlayHistory {


  val playsNum = 60  // 播放记录条数
  val days = 60   // 当前时间前N天的播放记录

  // word2vector参数
  val vectorDimension = 256  // 向量维度
  val windowSize = 10  //滑动窗口大小， 默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确


  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val trainTime = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)

    val df_train_users = getTrainUser(spark, trainTime)
    printDf("df_train_users", df_train_users)

    val df_train_ids = df_train_users.select(Dic.colUserId)

    // 3 Get play history in a past period of time

    val df_train_play = getPlayHistory(df_plays, trainTime, df_train_ids, 40, df_medias)
//    saveDataForXXK(df_train_play, "train", "train_userprofile_play_" + playsNum)

    printDf("输出 df_tran_play", df_train_play)

  }

  def getPlayHistory(df_play: DataFrame, trainTime: String, df_train_ids : DataFrame, playsNum: Int, df_medias:DataFrame) = {

    /**
     * 训练集用户一段时间的播放历史前N条记录
     */

    // 一段时间的播放历史
    val df_plays_part = df_play.filter(
      col(Dic.colPlayStartTime) < (trainTime)
        && col(Dic.colPlayStartTime) >= calDate(trainTime, - days)
    )

    // 训练集用户的播放历史
    val df_train_plays = df_plays_part.join(df_train_ids, Seq(Dic.colUserId), "inner")

    // 对每个用户选取前N条播放历史
    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)

    val df_train_select = df_train_plays
      .withColumn(Dic.colUserPlayCount,
        row_number.over(win1)
      ).filter(
      col(Dic.colUserPlayCount) .< (playsNum)
    )
//      .drop(Dic.colUserPlayCount)


    printDf("df_train_select", df_train_select)


//    saveDataForXXK(df_train_select, "train", "train_play_history_40")

    /**
     * 相应播放历史的媒资数据处理
     */

    val df_video_ids = df_train_select.select(Dic.colVideoId).dropDuplicates()

    val df_medias_part = df_medias.join(df_video_ids, Seq(Dic.colVideoId), "inner")
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId) > 0, 1).otherwise(0))
      .na.fill(
      Map((Dic.colVideoTitle, " "), (Dic.colIntroduction, " "))
    )
      .withColumn(
        "brief", udfRemovePunctuation(col(Dic.colVideoTitle), col(Dic.colIntroduction))
      )


    printDf("df_medias_part", df_medias_part)

    // 对videoId进行word2vec

    val df_medias_id = df_medias_part.agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoId + "_list"))

    printDf("df_medias_id", df_medias_id)

    val df_medias_w2v = w2vec(df_medias_id)
    printDf("df_medias_w2v", df_medias_w2v)

//    saveDataForXXK(df_medias_w2v, "train", "medias_in_play_history_" + playsNum + "_train_w2vec")


    val df_play_history = df_train_select
      .join(df_medias_part, Seq(Dic.colVideoId), "left")

    val df_free_play = df_play_history.filter(col(Dic.colInPackage) === 0)
    val df_paid_play = df_play_history.filter(col(Dic.colInPackage) === 1)

    val df_free_col = getUsersPlayColumns(df_free_play, "free", playsNum/2)
    val df_paid_col = getUsersPlayColumns(df_paid_play, "paid", playsNum/2)


    df_train_ids
      .join(df_free_col, Seq(Dic.colUserId), "left")
      .join(df_paid_col, Seq(Dic.colUserId), "left")
      .na.fill(0)

  }

  def getUsersPlayColumns(df_play: DataFrame, colName: String, topNPlay: Int) = {


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



  def w2vec(df_medias:DataFrame) ={

    val w2vModel = new Word2Vec()
      .setInputCol(Dic.colVideoId + "_list")
      .setOutputCol(Dic.colVector)
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(0)
    val model = w2vModel.fit(df_medias)
    model.getVectors
  }



  // 去除medais的brief列中的标点符号
  def udfRemovePunctuation = udf ( removePunctuation _)
  def removePunctuation(videoTitle: String, introduction: String) = {
    val regex = "[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]".r
    val brief = videoTitle + introduction
    val new_brief = regex.replaceAllIn(brief, "")

    if (new_brief.length < 126)
      new_brief
    else
      new_brief.substring(0,126)


  }


}
