package train.userpay

import mam.GetSaveData.{getDataFromXXK, getProcessedMedias, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf, when}

import scala.collection.mutable

object GetAllMediasText {

  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2
    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)


    val df_all_mediasText = getAllMediasText(df_medias)
    printDf("df_all_mediasText", df_all_mediasText)


    val df_have_bert = getDataFromXXK("common", "train_predict_text")
    printDf("df_have_bert", df_have_bert)

    val df_need_bert = df_all_mediasText.select(Dic.colVideoId)
      .except(df_have_bert.select(Dic.colVideoId))
      .join(df_all_mediasText, Seq(Dic.colVideoId), "inner")
      .dropDuplicates(Dic.colVideoId)

    printDf("df_need_bert", df_need_bert)

    saveDataForXXK(df_need_bert, "common", "need_bert_text")



  }

  def getAllMediasText(df_medias_all: DataFrame) = {

    // 对这部分媒资信息进行处理
    val df_medias_text = df_medias_all
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
      .select(Dic.colVideoId, Dic.colConcatText)

    df_medias_text
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


}
