package train.userpay

import mam.Dic
import mam.Utils.{getLabelAndCount, printDf, udfGetLabelAndCount}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat_ws, explode, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions.asJavaCollection

/**
 * @author wx
 * @param
 * @return
 * @describe  medias 中 video vector生成
 */
object MediasVideoVectorProcess {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .master("local[4]")
      .appName("MediasVideoVectorProcess")
      .getOrCreate()

    //val hdfsPath = "hdfs:///pay_predict/"
    val hdfsPath = ""

    val mediasProcessedPath = hdfsPath + "data/predict/common/processed/mediastemp" //HDFS路径
    var mediasProcessed = getProcessedMedias(mediasProcessedPath, spark)

    printDf("获取 medias", mediasProcessed)

    //对二级分类列表和标签列表内容进行提取

    //合并label和一二级分类列表
    import org.apache.spark.sql.functions._
    val separator = ","

    mediasProcessed = mediasProcessed.withColumn("labels", concat_ws(separator, col(Dic.colVideoOneLevelClassification), col(Dic.colVideoTwoLevelClassificationList), col(Dic.colVideoTagList)).cast(StringType))
    printDf("mediasProcessed", mediasProcessed)

  }
  def getProcessedMedias(mediasProcessedPath: String, spark: SparkSession): DataFrame = {

    val mediaSchema = StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colVideoTitle, StringType),
        StructField(Dic.colVideoOneLevelClassification, StringType),
        StructField(Dic.colVideoTwoLevelClassificationList, StringType),
        StructField(Dic.colVideoTagList, StringType),
        StructField(Dic.colDirectorList, StringType),
        StructField(Dic.colActorList, StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField(Dic.colReleaseDate, StringType),
        StructField(Dic.colStorageTime, StringType),
        //视频时长
        StructField(Dic.colVideoTime, StringType),
        StructField(Dic.colScore, StringType),
        StructField(Dic.colIsPaid, StringType),
        StructField(Dic.colPackageId, StringType),
        StructField(Dic.colIsSingle, StringType),
        //是否片花
        StructField(Dic.colIsTrailers, StringType),
        StructField(Dic.colSupplier, StringType),
        StructField(Dic.colIntroduction, StringType)
      )
    )

    val medias = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(mediaSchema)
      .csv(mediasProcessedPath)

    medias
  }






  def getVector(df:DataFrame, listName:String) ={

    val vectorDimension = 64
    val windowSize = 10  //默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
    val w2vModel = new Word2Vec()
      .setInputCol(listName)
      .setOutputCol(listName + "_vector")
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(5)  //???
    val model = w2vModel.fit(df)

    //print("滑动窗口的大小：" + w2vModel.getWindowSize)
    //val result = model.transform(playsList)
    model.getVectors
  }



}
