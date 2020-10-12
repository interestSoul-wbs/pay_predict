package train.common

import mam.Dic
import org.apache.avro.SchemaBuilder.array
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, udf}
import org.apache.spark.ml.feature.Word2Vec
//import org.apache.spark.ml.feature.V

object PlayHistoryProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrdersProcess")
      .master("local[6]")
      .getOrCreate()
    val hdfsPath=""
    val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val plays = spark.read.format("parquet").load(playsProcessedPath)
    val playsList=plays.groupBy(col(Dic.colUserId)).agg(collect_list(col(Dic.colVideoId)).as("video_list"))

    val vectorDimension=64
    val w2vModel=new Word2Vec()
      .setInputCol("video_list")
      .setOutputCol("result")
      .setVectorSize(vectorDimension)
      .setMinCount(0)
    val model=w2vModel.fit(playsList)
    //val result=model.transform(playsList)
    var videoDict=model.getVectors

    def break(array:Object,index:Int) ={
      val vectorString=array.toString
      vectorString.substring(1,vectorString.length-1).split(",")(index).toDouble
      //(Vector)

    }
    def udfBreak=udf(break _)
    for(i <- 0 to vectorDimension-1)
      videoDict=videoDict.withColumn("v_"+i,udfBreak(col("vector"),lit(i)))

    val videoVectorPath=hdfsPath+"data/train/common/processed/videoVector"+args(0)
    videoDict.write.mode(SaveMode.Overwrite).format("parquet").save(videoVectorPath)
    //wordDict

  }

}
