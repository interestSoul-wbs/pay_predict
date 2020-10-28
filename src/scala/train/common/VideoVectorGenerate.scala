package train.common

import mam.Dic
import mam.Utils.{printDf, udfBreak}
import org.apache.avro.SchemaBuilder.array
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, udf}
import org.apache.spark.ml.feature.Word2Vec
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
    val plays = spark.read.format("parquet").load(playsProcessedPath)
    val playsList=plays
      .filter(col(Dic.colPlayEndTime).<(args(0)+" "+args(1)))
      .groupBy(col(Dic.colUserId))
      .agg(collect_list(col(Dic.colVideoId)).as("video_list"))
    printDf("plays",plays)

    val vectorDimension=64
    val w2vModel=new Word2Vec()
      .setInputCol("video_list")
      .setOutputCol("result")
      .setVectorSize(vectorDimension)
      .setMinCount(5)
    val model=w2vModel.fit(playsList)
    //val result=model.transform(playsList)
    var videoDict=model.getVectors


    for(i <- 0 to vectorDimension-1)
      videoDict=videoDict.withColumn("v_"+i,udfBreak(col("vector"),lit(i))).withColumnRenamed("word",Dic.colVideoId)

    printDf("videoDict",videoDict)
    val videoVectorPath=hdfsPath+"data/train/common/processed/videovector"+args(0)
    videoDict.write.mode(SaveMode.Overwrite).format("parquet").save(videoVectorPath)
    //wordDict

  }

}
