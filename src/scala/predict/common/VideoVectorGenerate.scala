package predict.common

import mam.GetSaveData.{getProcessedMedias, getProcessedPlay, saveVideoVector}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting, udfSortByPlayTime}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object VideoVectorGenerate {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)
    val df_medias=getProcessedMedias(spark)
    printDf("输入 df_media",df_medias)
    val df_plays=getProcessedPlay(spark)
    printDf("输入 df_play",df_plays)

    //3 Process Data
    //构建视频列表
    val playsList=getPlayList(df_plays,now,df_medias)
    //用户每月平均观看视频32个
    printDf("plays_list",playsList)
    //得到单点视频的嵌入向量以及每个单点视频最为相似的20个单点视频作为召回
    val videoVectorAndSims=getVectorAndSims(playsList)
    printDf("videoVectorAndSims",videoVectorAndSims)


    //4 Save Data

    saveVideoVector(now,videoVectorAndSims,"predict")
    println("VideoVectorGenerate  over~~~~~~~~~~~")

  }





  def getPlayList(plays:DataFrame,now:String,medias:DataFrame) ={
    val joinKeysVideoId=Seq(Dic.colVideoId)
    var playsList=plays.withColumn("play_month",substring(col(Dic.colPlayEndTime),0,7))
    playsList=playsList.join(medias.select(col(Dic.colVideoId),
      col(Dic.colVideoOneLevelClassification),col(Dic.colIsPaid)),joinKeysVideoId,"inner")
    playsList=playsList
      .filter(
        col(Dic.colPlayEndTime).<(now)
          && col(Dic.colBroadcastTime).>(360)
      && (col(Dic.colIsPaid).===(1) || col(Dic.colVideoOneLevelClassification).===("电影")))
      .groupBy(col(Dic.colUserId),col("play_month"))
      .agg(udfSortByPlayTime(collect_list(struct(col(Dic.colVideoId),col(Dic.colPlayEndTime)))).as("video_list"))
      .select(col(Dic.colUserId),col("video_list"))
    playsList
  }


  def getVectorAndSims(playsList:DataFrame) ={

    val vectorDimension=64
    val windowSize=10  //默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
    val w2vModel=new Word2Vec()
      .setInputCol(Dic.colVideoList)
      .setOutputCol(Dic.colResult)
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(0)
      .setMaxSentenceLength(10)

    val model=w2vModel.fit(playsList)

    val vecAndSimilarity=model.getVectors

    def udfGetSimItems=udf(getSimItems _)
    def getSimItems(word:String)={
      val temp=model.findSynonymsArray(word, 5)
      val result=temp.map(item=>item._1)
      result
    }

    vecAndSimilarity.withColumnRenamed(Dic.colWord,Dic.colVideoId).withColumn(Dic.colSimVideos,udfGetSimItems(col(Dic.colVideoId)))

  }

}
