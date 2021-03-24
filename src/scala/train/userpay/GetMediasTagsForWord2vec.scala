package train.userpay

import mam.GetSaveData.{getDataFromXXK, getProcessedMedias, getProcessedPlay, getTrainUser, hdfsPath, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfBreakList, udfGetTopNHistory, udfLpad}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit, row_number, sort_array, udf, when}
import train.userpay.GetMediasForBertAndPlayList.playsNum

import scala.collection.mutable

object GetMediasTagsForWord2vec {

  val days = 60 // 当前时间前N天的播放记录

  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val df_medias = getProcessedMedias(spark)
    printDf("Input df_medias", df_medias)

    val df_train_medias_bert = getDataFromXXK("train", "train_medias_" + playsNum)
    val df_train_id = df_train_medias_bert.select(Dic.colVideoId)

    printDf("Input df_train_medias_bert", df_train_medias_bert)

    val df_predict_medias_bert = getDataFromXXK("predict", "predict_medias_" + playsNum)
    val df_predict_id = df_predict_medias_bert.select(Dic.colVideoId)

    printDf("Input df_predict_medias_bert", df_medias)

    val df_train_part = getMediasForWord2Vec(df_train_id, df_medias)
    printDf("Output df_train_part", df_train_part)
    //    saveDataForXXK(df_train_part, "train", "train_medias_w2v")

    val df_predict_part = getMediasForWord2Vec(df_predict_id, df_medias)
    printDf("Output df_predict_part", df_predict_part)
    //    saveDataForXXK(df_predict_part, "predict", "predict_medias_w2v")


    val df_medias_w2v = df_train_part.union(df_predict_part).dropDuplicates()

    saveDataForXXK(df_medias_w2v, "train", "/common/medias_labels_60")


  }

  def getMediasForWord2Vec(df_video_id: DataFrame, df_medias: DataFrame) = {
    val df_medias_2vec = df_video_id.join(df_medias, Seq(Dic.colVideoId), "inner")
    val df_concat_tags = df_medias_2vec
      .na.fill(" ")
      .select(
        col(Dic.colVideoId), col(Dic.colVideoTitle), col(Dic.colVideoOneLevelClassification), col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction),
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList)).otherwise(Array("null")).as(Dic.colVideoTwoLevelClassificationList),
        when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)).otherwise(Array("null")).as(Dic.colVideoTagList),
        when(col(Dic.colActorList).isNotNull, col(Dic.colActorList)).otherwise(Array("null")).as(Dic.colActorList),
        when(col(Dic.colDirectorList).isNotNull, col(Dic.colDirectorList)).otherwise(Array("null")).as(Dic.colDirectorList)
      )
      .withColumn(
        Dic.colConcatTags,
        udfRemovePunctuation(
          col(Dic.colVideoTitle), col(Dic.colVideoOneLevelClassification), col(Dic.colVideoTwoLevelClassificationList), col(Dic.colVideoTagList),
          col(Dic.colActorList), col(Dic.colDirectorList), col(Dic.colCountry), col(Dic.colLanguage), col(Dic.colIntroduction)
        )
      )
      .select(col(Dic.colVideoId), col(Dic.colConcatTags))

    df_concat_tags

  }


  // 去除medais的brief列中的标点符号
  def udfRemovePunctuation = udf(removePunctuation _)

  def removePunctuation(videoTitle: String, oneLable: String, twoLable: mutable.WrappedArray[String], tags: mutable.WrappedArray[String],
                        actorList: mutable.WrappedArray[String], directorList: mutable.WrappedArray[String], country: String, language: String,
                        introduction: String) = {

    val regex = "[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×WrappedArray null\t]".r

    val brief = videoTitle + oneLable + twoLable.toString + tags.toString + actorList.toString + directorList.toString + country + language + introduction
    val new_brief = regex.replaceAllIn(brief, "")

    if (new_brief.length < 126)
      new_brief
    else
      new_brief.substring(0, 126)


  }


}
