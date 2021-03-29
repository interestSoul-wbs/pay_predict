package train.userpay


import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, getTrainUser, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfBreakList, udfGetTopNHistory, udfLpad}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, max, row_number, sort_array, udf, when}
import org.apache.spark.sql.types.StringType
import train.userpay.GetMediasVector.{vectorDimension, windowSize}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GetMediasForBertAndPlayList {

  val days = 60 // 当前时间前N天的播放记录
  val playsNum = 60

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

    val df_train_select = getUserPartTimeTopNPlay(df_plays, df_train_ids, trainTime, days)


//    val df_videoId2Vec = getPlayW2V(df_train_select)
//    saveDataForXXK(df_videoId2Vec, "train", "train_videoId_w2v" )


    // Medias text info Bert
    val df_medias_part = getPartMediasForBert(df_train_select, df_medias)
//    saveDataForXXK(df_medias_part.select(Dic.colVideoId, Dic.colConcatText), "train", "train_medias_text_" + playsNum )

//    //Users' play list
//    val df_play_list = getUserFreePaidPlayList(df_train_select, df_medias_part, df_train_ids)
//    saveDataForXXK(df_play_list, "train", "train_play_free_paid_" + playsNum )


  }


  def getUserPartTimeTopNPlay(df_play: DataFrame, df_user_id: DataFrame, time: String, days: Int) = {

    // 一段时间的播放历史
    val df_plays_part = df_play.filter(
      col(Dic.colPlayStartTime) < (time)
        && col(Dic.colPlayStartTime) >= calDate(time, -days)
    )

    // 训练集用户的播放历史
    val df_user_plays = df_plays_part.join(df_user_id, Seq(Dic.colUserId), "inner")

    // 对每个用户选取前N条播放历史
    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)

    val df_play_select = df_user_plays
      .withColumn(Dic.colUserPlayCount,
        row_number.over(win1)
      ).filter(
      col(Dic.colUserPlayCount).<(playsNum)
    )


    printDf("df_play_select", df_play_select)

    df_play_select

  }


  def getPlayW2V(df_play_select: DataFrame) = {


    //获取数字位数
    val rowCount = df_play_select.count().toString.length
    println("df count number length", rowCount)

    val win = Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayStartTime))

    /**
     * This part is to ensure the sequence has correct order after spark cluster
     * The order is desc(play_start_time)
     */
    val df_play_temp = df_play_select.withColumn(Dic.colUserPlayCount, row_number().over(win))
      //为播放的视频id进行排序
      .withColumn("0", lit("0"))
      .withColumn("tmp_rank", udfLpad(col(Dic.colUserPlayCount), lit(rowCount), col("0"))) //拼接列
      .drop("0")
      .withColumn("tmp_column", concat_ws(":", col("tmp_rank"), col(Dic.colVideoId)))
      //将list中的video按照播放次序排列
      .groupBy(col(Dic.colUserId))
      .agg(collect_list(col("tmp_column")).as("tmp_column")) //collect_set 会去重
      .withColumn("tmp_column_1", sort_array(col("tmp_column")))
      .withColumn(Dic.colPlayList, udfProcessPlayList(col("tmp_column_1")))
      .select(Dic.colUserId, Dic.colPlayList)


    printDf("df_play_temp", df_play_temp)

    // Word2Vec
    val df_play_w2v = w2vec(df_play_temp)

    printDf("df_play_w2v", df_play_w2v)

    df_play_w2v

  }


  def udfProcessPlayList = udf(progressPlayList _)

  def progressPlayList(array: mutable.WrappedArray[String]) = {
    val result = new ListBuffer[String]()

    for (ele <- array) {
        result.append(ele.split(":")(1))
    }

    result
  }




  def getPartMediasForBert(df_play_select: DataFrame, df_medias_all: DataFrame) = {

    val df_video_ids = df_play_select.select(Dic.colVideoId).dropDuplicates()

    // 对这部分媒资信息进行处理
    val df_medias_part = df_medias_all
      .join(df_video_ids, Seq(Dic.colVideoId), "inner")
      .select(
        col(Dic.colVideoId), col(Dic.colVideoTitle),
        col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction),
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList)).otherwise(Array(" ")).as(Dic.colVideoTwoLevelClassificationList),
        when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)).otherwise(Array(" ")).as(Dic.colVideoTagList),
        when(col(Dic.colActorList).isNotNull, col(Dic.colActorList)).otherwise(Array(" ")).as(Dic.colActorList),
        when(col(Dic.colDirectorList).isNotNull, col(Dic.colDirectorList)).otherwise(Array(" ")).as(Dic.colDirectorList),
        when(col(Dic.colPackageId) > 0, 1).otherwise(0).as(Dic.colInPackage)
      )
      .na.fill(
      Map((Dic.colVideoTitle, " "), (Dic.colCountry, " "), (Dic.colLanguage, " "), (Dic.colIntroduction, " "))
    )
      .withColumn(
        Dic.colConcatText,
        udfRemovePunctuation(
          col(Dic.colVideoTitle), col(Dic.colVideoTwoLevelClassificationList), col(Dic.colVideoTagList),
          col(Dic.colActorList), col(Dic.colDirectorList), col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction)
        )
      )
      .select(Dic.colVideoId, Dic.colInPackage, Dic.colConcatText)


    printDf("df_medias_part", df_medias_part)
    df_medias_part
  }


  def getUserFreePaidPlayList(df_play_select: DataFrame, df_medias_part: DataFrame, df_user_id: DataFrame) = {

    val df_play_history = df_play_select
      .join(df_medias_part, Seq(Dic.colVideoId), "left")

    val df_free_play = df_play_history.filter(col(Dic.colInPackage) === 0)
    val df_paid_play = df_play_history.filter(col(Dic.colInPackage) === 1)

    val df_free_col = getUsersPlayList(df_free_play, "free", playsNum / 2)
    val df_paid_col = getUsersPlayList(df_paid_play, "paid", playsNum / 2)


    val df_play_list = df_user_id
      .join(df_free_col, Seq(Dic.colUserId), "left")
      .join(df_paid_col, Seq(Dic.colUserId), "left")
      .na.fill("0")

    df_play_list

  }


  // 去除medais的brief列中的标点符号
  def udfRemovePunctuation = udf(removePunctuation _)

  def removePunctuation(videoTitle: String, twoLable: mutable.WrappedArray[String], tags: mutable.WrappedArray[String],
                        actorList: mutable.WrappedArray[String], directorList: mutable.WrappedArray[String], country: String, language: String,
                        introduction: String) = {

    val regex = "[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×WrappedArray \t]".r

    val brief = videoTitle + twoLable.toString + tags.toString + actorList.toString + directorList.toString + country + language + introduction
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

    for (i <- 0 to topNPlay - 1)
      df_play_data = df_play_data.withColumn(colName + i, udfBreakList(col(colName + "_list"), lit(i)))

    printDf("df_play_data", df_play_data)

    df_play_data.drop(colName + "_list")

  }


  //
  //  def getMediasForBertAndPlayList(df_play: DataFrame, df_medias: DataFrame, df_train_ids: DataFrame, trainTime: String, state: String) = {
  //
  //    // 一段时间的播放历史
  //    val df_plays_part = df_play.filter(
  //      col(Dic.colPlayStartTime) < (trainTime)
  //        && col(Dic.colPlayStartTime) >= calDate(trainTime, -days)
  //    )
  //
  //    // 训练集用户的播放历史
  //    val df_train_plays = df_plays_part.join(df_train_ids, Seq(Dic.colUserId), "inner")
  //
  //    // 对每个用户选取前N条播放历史
  //    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colPlayStartTime)
  //
  //    val df_train_select = df_train_plays
  //      .withColumn(Dic.colUserPlayCount,
  //        row_number.over(win1)
  //      ).filter(
  //      col(Dic.colUserPlayCount).<(playsNum)
  //    )
  //
  //
  //    printDf("df_train_select", df_train_select)
  //
  //
  //    /**
  //     * 相应播放历史的媒资数据处理
  //     * 处理之后的文本信息进行bert
  //     */
  //    // 获取这部分用户的播放ID
  //    val df_video_ids = df_train_select.select(Dic.colVideoId).dropDuplicates()
  //
  //    // 对这部分媒资信息进行处理
  //    val df_medias_part = df_medias.join(df_video_ids, Seq(Dic.colVideoId), "inner")
  //      .select(
  //        col(Dic.colVideoId), col(Dic.colVideoTitle),
  //        col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction),
  //        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList)).otherwise(Array(" ")).as(Dic.colVideoTwoLevelClassificationList),
  //        when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)).otherwise(Array(" ")).as(Dic.colVideoTagList),
  //        when(col(Dic.colActorList).isNotNull, col(Dic.colActorList)).otherwise(Array(" ")).as(Dic.colActorList),
  //        when(col(Dic.colDirectorList).isNotNull, col(Dic.colDirectorList)).otherwise(Array(" ")).as(Dic.colDirectorList),
  //        when(col(Dic.colPackageId) > 0, 1).otherwise(0).as(Dic.colInPackage)
  //      )
  //      .na.fill(
  //      Map((Dic.colVideoTitle, " "), (Dic.colCountry, " "), (Dic.colLanguage, " "), (Dic.colIntroduction, " "))
  //    )
  //      .withColumn(
  //        Dic.colConcatText,
  //        udfRemovePunctuation(
  //          col(Dic.colVideoTitle), col(Dic.colVideoTwoLevelClassificationList), col(Dic.colVideoTagList),
  //          col(Dic.colActorList), col(Dic.colDirectorList), col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction)
  //        )
  //      )
  //      .select(Dic.colVideoId, Dic.colInPackage, Dic.colConcatText)
  //
  //
  //    printDf("df_medias_part", df_medias_part)
  //
  //
  //    // Save Medias for Bert
  //    saveDataForXXK(df_medias_part, state, state + "_medias_text_" + playsNum)
  //    /**
  //     *
  //     */
  //
  //    val df_play_history = df_train_select
  //      .join(df_medias_part, Seq(Dic.colVideoId), "left")
  //
  //    val df_free_play = df_play_history.filter(col(Dic.colInPackage) === 0)
  //    val df_paid_play = df_play_history.filter(col(Dic.colInPackage) === 1)
  //
  //    val df_free_col = getUsersPlayList(df_free_play, "free", playsNum / 2)
  //    val df_paid_col = getUsersPlayList(df_paid_play, "paid", playsNum / 2)
  //
  //
  //    val df_play_list = df_train_ids
  //      .join(df_free_col, Seq(Dic.colUserId), "left")
  //      .join(df_paid_col, Seq(Dic.colUserId), "left")
  //      .na.fill("0")
  //
  //
  //    // Save Users' play list
  //    saveDataForXXK(df_play_list, "train", "train_play_free_paid_" + playsNum)
  //  }


  // word2vec
  def w2vec(df_medias: DataFrame) = {

    val w2vModel = new Word2Vec()
      .setInputCol(Dic.colPlayList)
      .setOutputCol(Dic.colVector)
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(0)
    val model = w2vModel.fit(df_medias)

    model.getVectors
      .withColumnRenamed("word", Dic.colVideoId)
  }


}
