package train.common

import mam.Dic
import mam.GetSaveData.{getProcessedMedias, getProcessedPlays}
import mam.Utils.{printDf, udfBreak, udfSortByPlayTime}
import org.apache.avro.SchemaBuilder.array
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, struct, substring, udf}
import org.apache.spark.ml.feature.Word2Vec
import predict.common.VideoVectorGenerate.{getData, getPlayList, getVector}
//import org.apache.spark.ml.feature.V

object VideoVectorGenerate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("VideoVectorGenerate")
      //.master("local[6]")
      .getOrCreate()
    //val hdfsPath=""
    val hdfsPath="hdfs:///pay_predict/"
    val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val mediasProcessedPath=hdfsPath+"data/train/common/processed/mediastemp"
    val now=args(0)+" "+args(1)
    val plays = getProcessedPlays(spark,playsProcessedPath)
    val medias=getProcessedMedias(spark,mediasProcessedPath)
    printDf("plays",plays)
    //构建视频列表
    val playsList=getPlayList(plays,now,medias)
    //用户每月平均观看视频32个
    printDf("plays_list",playsList)
    var videoDict=getVector(playsList,medias)

    val vectorDimension=64
    for(i <- 0 to vectorDimension-1)
      videoDict=videoDict.withColumn("v_"+i,udfBreak(col("vector"),lit(i))).withColumnRenamed("word",Dic.colVideoId)

    printDf("videoVector",videoDict)
    val videoVectorPath=hdfsPath+"data/train/common/processed/videovector"+args(0)
    videoDict.write.mode(SaveMode.Overwrite).format("parquet").save(videoVectorPath)
    //wordDict

  }
  def getData(spark:SparkSession,playsProcessedPath:String)={
    spark.read.format("parquet").load(playsProcessedPath)
  }



  /**
   *
   * @param plays
   * @param now
   * @return
   */
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

  /**
   *
   * @param playsList
   * @return
   */
  def getVector(playsList:DataFrame,medias:DataFrame) ={

    val vectorDimension=64
    val windowSize=10  //默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
    val w2vModel=new Word2Vec()
      .setInputCol("video_list")
      .setOutputCol("result")
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(5)
      .setMaxSentenceLength(10)

    val model=w2vModel.fit(playsList)
//查看embedding向量的效果
//    //星球大战9：天行者崛起（普通话）
//    var synonyms = model.findSynonyms("11036188653", 20).withColumnRenamed("word",Dic.colVideoId)
//    val joinKeysVideoId=Seq(Dic.colVideoId)
//    var simVideos=synonyms.join(medias.select(col(Dic.colVideoId),col(Dic.colVideoTitle)),joinKeysVideoId,"left")
//    simVideos.orderBy("similarity").show()
//    //欢乐好声音（普通话）
//     synonyms = model.findSynonyms("11012215829", 20).withColumnRenamed("word",Dic.colVideoId)
//     simVideos=synonyms.join(medias.select(col(Dic.colVideoId),col(Dic.colVideoTitle)),joinKeysVideoId,"left")
//     simVideos.orderBy("similarity").show()
//
//    //X战警：逆转未来（普通话）（HDR）
//    synonyms = model.findSynonyms("11014191793", 20).withColumnRenamed("word",Dic.colVideoId)
//    simVideos=synonyms.join(medias.select(col(Dic.colVideoId),col(Dic.colVideoTitle)),joinKeysVideoId,"left")
//    simVideos.orderBy("similarity").show()
//    //铁血战士
//    synonyms = model.findSynonyms("11030923253", 20).withColumnRenamed("word",Dic.colVideoId)
//    simVideos=synonyms.join(medias.select(col(Dic.colVideoId),col(Dic.colVideoTitle)),joinKeysVideoId,"left")
//    simVideos.orderBy("similarity").show()
//
//    val first=model.getVectors.head().getAs[String]("word")
//    synonyms = model.findSynonyms(first, 20).withColumnRenamed("word",Dic.colVideoId)
//    simVideos=synonyms.join(medias.select(col(Dic.colVideoId),col(Dic.colVideoTitle)),joinKeysVideoId,"left")
//    simVideos.orderBy("similarity").show()
    
    model.getVectors
  }

}
