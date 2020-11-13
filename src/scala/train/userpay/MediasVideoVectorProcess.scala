package train.userpay

import mam.Dic
import mam.Utils.{printArray, printDf, udfBreak, udfConcatLabels, udfGetDays, udfLongToTimestamp, udfLongToTimestampV2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature.{Imputer, Word2Vec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import train.common.MediasProcess.{getArrayStrColLabelAndSave, getSingleStrColLabelAndSave}

import scala.collection.JavaConversions.asJavaCollection
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

    //val mediasProcessedPath = hdfsPath + "data/predict/common/processed/mediastemp" //HDFS路径
    val mediasProcessedPath = "data/train/common/raw/medias/medias.txt"
    var mediasProcessed = getProcessedMedias(mediasProcessedPath, spark)
    mediasProcessed = mediasProcess(mediasProcessed)  // !!!!!!!!!!!!!!!! 记得去掉
    printDf("获取 medias", mediasProcessed)



    //对标签进行合并

    mediasProcessed = mediasProcessed.withColumn("labels", udfConcatLabels(col(Dic.colVideoOneLevelClassification), col(Dic.colVideoTwoLevelClassificationList), col(Dic.colVideoTagList)))

    // 标签和其对应的vector
    val vectorDimension = 32
    var labelVector = getVector(mediasProcessed, vectorDimension)
    printDf("Label vector", labelVector)




//    val videoVector = mediasProcessed.withColumn("video_vector", udfGetVideoVector(col(Dic.colLabels), labelVector))
//    var labels = mediasProcessed.select(collect_list("labels"))
//
//    val mutableset = ArrayBuffer[String]()
//    var nums: Map[String, Int] = Map()
//
//      labels.foreach(row=>{
//      val rs = row.getList(0)  //只有一行
//        println(rs)
//      for (i <- 0 to rs.size() - 1) {
//        val temp:mutable.WrappedArray[String] = rs.get(i)
//        temp.foreach({ i =>
//          nums += (i -> 1)
//        })
//      }
//      })
//
//    println(nums.keys.size)

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
        //StructField(Dic.colInPackage, IntegerType)

      )
    )

    val medias = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(mediaSchema)
      .csv(mediasProcessedPath)

    medias

  }

  def mediasProcess(rawMediasData:DataFrame)={
    /**
     *@author wj
     *@param [rawMediasData]
     *@return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     *@description  更改数据格式，主要将NULL字符串转化为null或者NaN
     */
    var dfModifiedFormat = rawMediasData.select(
      when(col(Dic.colVideoId)==="NULL",null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
      when(col(Dic.colVideoTitle)==="NULL",null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
      when(col(Dic.colVideoOneLevelClassification)==="NULL",null).otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
      from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
      from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
      from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
      from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
      when(col(Dic.colCountry)==="NULL",null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
      when(col(Dic.colLanguage)==="NULL",null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
      when(col(Dic.colReleaseDate)==="NULL",null).otherwise(col(Dic.colReleaseDate) ).as(Dic.colReleaseDate),
      when(col(Dic.colStorageTime)==="NULL",null).otherwise(udfLongToTimestampV2(col(Dic.colStorageTime ))).as(Dic.colStorageTime),
      when(col(Dic.colVideoTime)==="NULL",Double.NaN).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
      when(col(Dic.colScore)==="NULL",Double.NaN).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
      when(col(Dic.colIsPaid)==="NULL",Double.NaN).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
      when(col(Dic.colPackageId)==="NULL",null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
      when(col(Dic.colIsSingle)==="NULL",Double.NaN).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
      when(col(Dic.colIsTrailers)==="NULL",Double.NaN).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
      when(col(Dic.colSupplier)==="NULL",null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
      when(col(Dic.colIntroduction)==="NULL",null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction)
    )

    //是否单点 是否付费填充
    dfModifiedFormat = dfModifiedFormat.na.fill(Map((Dic.colIsSingle, 0),(Dic.colIsPaid, 0)))
      //添加新列 是否在套餐内
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId).>(0), 1).otherwise(0))
    dfModifiedFormat

  }


  def getVector(df:DataFrame, vectorDimension:Int) ={
    /**
     *@author wj
     *@param [playsList]
     *@return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     *@description  训练Word2vector模型，得到视频的嵌入向量
     */
    //val windowSize = 10  //默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
    val w2vModel = new Word2Vec()
      .setInputCol("labels")
      .setOutputCol("labels_vector")
      .setVectorSize(vectorDimension)
      .setMinCount(0)
      //.setWindowSize(windowSize)
    val model = w2vModel.fit(df)

    //print("滑动窗口的大小："+w2vModel.getWindowSize)
    //val result=model.transform(playsList)
    model.getVectors
  }

}
