package train.userpay

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedMedias, getProcessedPlay}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object test {

  val predictResourceId = Array(100201, 100202) //要预测的套餐id

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    val df_plays = getProcessedPlay(spark)
    printDf("df_plays", df_plays)
    val df_medias = getProcessedMedias(spark)
    printDf("df_medias", df_medias)

    recallVideosGenerate(df_plays, df_medias)

  }


  def recallVideosGenerate(df_plays: DataFrame, df_medias: DataFrame): Unit = {

    val joinKeysVideoId = Seq(Dic.colVideoId)
    val df_dataset = df_plays
      .join(df_medias.select(col(Dic.colVideoId), col(Dic.colVideoTitle)), joinKeysVideoId, "inner")
      .filter(
        col(Dic.colTimeSum).>(360)
          && col(Dic.colPackageId).isin(predictResourceId: _*)
      )
      .select(col(Dic.colUserId), col(Dic.colVideoId), col(Dic.colVideoTitle))
      .withColumn(Dic.colIndex, lit(1))

    df_dataset.show()

    //对user_id和video_id进行label_encode编码
    val userIDIndexer = new StringIndexer()
      .setInputCol(Dic.colUserId)
      .setOutputCol(Dic.colUserIdIndex)

    val videoIDIndexer = new StringIndexer()
      .setInputCol(Dic.colVideoId)
      .setOutputCol(Dic.colVideoIdIndex)

    val df_dataset_user_index = userIDIndexer
      .fit(df_dataset)
      .transform(df_dataset)

    val df_dataset_indexed = videoIDIndexer
      .fit(df_dataset_user_index)
      .transform(df_dataset_user_index)

    val df_user_index = df_dataset_indexed
      .select(col(Dic.colUserId), col(Dic.colUserIdIndex))
      .dropDuplicates()
      .orderBy(col(Dic.colUserIdIndex))

    val df_video_index = df_dataset_indexed
      .select(col(Dic.colVideoId), col(Dic.colVideoIdIndex))
      .dropDuplicates()
      .orderBy(col(Dic.colVideoIdIndex))

    df_user_index.show()
    df_video_index.show()

    //使用协同过滤算法
    val als = new ALS() //ALS 交替最小二乘法
      .setUserCol(Dic.colUserIdIndex) //userid
      .setItemCol(Dic.colVideoIdIndex) //itemid
      .setRatingCol(Dic.colIndex) //rating矩阵，这里跟你输入的字段名字要保持一致。很明显这里是显示评分得到的矩阵形式
      .setMaxIter(5)
      .setRegParam(0.01)
      .setImplicitPrefs(true) //此处表明rating矩阵是隐式评分
    val model = als.fit(df_dataset_indexed)

    model.itemFactors.show()
  }

}
