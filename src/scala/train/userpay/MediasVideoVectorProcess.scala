package train.userpay

import mam.Dic
import mam.Utils.{getData, printDf, saveProcessedData, udfBreak}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, collect_list, explode, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @author wx
 * @param
 * @return
 * @describe medias 中 video vector生成
 */
object MediasVideoVectorProcess {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
//      .master("local[4]")
      .appName("MediasVideoLabelsVectorProcess")
      .getOrCreate()

    val hdfsPath = "hdfs:///pay_predict/"
//    val hdfsPath = ""
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp" //HDFS路径
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasVectorSavePath = hdfsPath + "data/train/common/processed/userpay/mediasVector"


    var df_medias = getData(spark, mediasProcessedPath)
    println("Get Medias data", df_medias.count())
    val df_plays = getData(spark, playsProcessedPath)
    println("Get play data", df_plays.count())

    printDf("medias", df_medias)

    // Medias further process about Labels
    val df_mediasLabels = df_medias.dropDuplicates(Dic.colVideoId)
      .na.drop("all")
      // Deal with the NullPointerException
      .na.fill(Map((Dic.colVideoOneLevelClassification, "其他")))
      .withColumn(Dic.colVideoTwoLevelClassificationList,
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList)).otherwise(Array("其他")))
      .withColumn(Dic.colVideoTagList, when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)).otherwise(Array("其他")))
      .select(Dic.colVideoId, Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList)

    // 选择有play记录的video embedding
    val df_playsVideoId = df_plays.select(Dic.colVideoId).distinct()
    val df_mediasPlayed = df_mediasLabels.join(df_playsVideoId, Seq(Dic.colVideoId), "inner")
    println("Medias' videos played by users: ", df_mediasPlayed.count())


    val vectorDimension = 32

    /**
     * Get vectors for level one
     */
    val df_levelOneVector = getLevelOneVector(vectorDimension, df_mediasPlayed)
    printDf("Level one classification vector", df_levelOneVector)

    /**
     * Get mean vectors for level two classification list
     */
    val df_levelTwoMeanVector = getMeanVector(vectorDimension, df_mediasPlayed, Dic.colVideoTwoLevelClassificationList)
    printDf("Level two classification mean vector", df_levelTwoMeanVector)


    /**
     * Get mean vectors for tags list
     */

    val df_tagsMeanVector = getMeanVector(vectorDimension, df_mediasPlayed, Dic.colVideoTagList)
    printDf("Tags mean vector", df_tagsMeanVector)


    /**
     * Get video vector (vectorDimension is 37)
     */

    val df_videoVector = getVideoVector(vectorDimension, df_levelOneVector, df_levelTwoMeanVector, df_tagsMeanVector, df_mediasPlayed)
    printDf("video all labels vector", df_videoVector)


    /**
     * Video Vector include video hours and score, is_paid, in_package, Labels word2vector
     * storage_date_gap of train and predict time(add in TrainSet/PredictSet file)
     */

    df_medias = df_medias
      .withColumn(Dic.colVideoHour, col(Dic.colVideoTime) / 3600)
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId).>(0), 1).otherwise(0))
      .withColumn(Dic.colScore, col(Dic.colScore) / 10)
      .na.fill(Map((Dic.colIsPaid, 0), (Dic.colInPackage, 0)))

    // colVideoOneLevelClassification is used in file TrainSetProcess
    val df_mediasPart = df_medias.select(Dic.colVideoId, Dic.colScore, Dic.colIsPaid, Dic.colInPackage, Dic.colVideoHour, Dic.colStorageTime, Dic.colVideoOneLevelClassification)

    val df_mediasVecMultiCol = df_mediasPart.join(df_videoVector, Seq(Dic.colVideoId), "right")

    printDf("Medias' Vector Multiple Columns", df_mediasVecMultiCol)


    // Save data to HDFS
    saveProcessedData(df_mediasVecMultiCol, mediasVectorSavePath)
    println("Media's video vector saved !!! ")

  }


  def getVector(df: DataFrame, vectorDimension: Int, colName: String) = {
    /**
     * @author wj
     * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @description 训练Word2vector模型，得到视频的嵌入向量
     */
    val windowSize = 10 //默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
    val w2vModel = new Word2Vec()
      .setInputCol(colName) //编码的列名
      .setOutputCol("label_vector") //输出的对应向量
      .setVectorSize(vectorDimension)
      .setMinCount(0)
      .setWindowSize(windowSize)

    printDf(colName + " getVector DataFrame", df)

    val model = w2vModel.fit(df)

    model.getVectors
  }


  def getLevelOneVector(vectorDimension: Int, df_mediasPlayed: DataFrame) = {
    /**
     * @description: get vectors of video's colVideoOneLevelClassification
     * @param: vectorDimension
     * @param: df_mediasPlayed : medias videos played by users
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    // In function getVector fit() need to br Array[String] type
    val df_videoLevelOne = df_mediasPlayed
      .groupBy(col(Dic.colVideoId))
      .agg(collect_list(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification + "_list"))

    val df_labelVector_1 = getVector(df_videoLevelOne, vectorDimension, Dic.colVideoOneLevelClassification + "_list")
      .withColumnRenamed("word", Dic.colVideoOneLevelClassification)
    printDf("get level one vector", df_labelVector_1)

    //medias dataframe join with level one vector
    var df_levelOneBreak = df_mediasPlayed.join(df_labelVector_1, Seq(Dic.colVideoOneLevelClassification), "inner")
      .select(Dic.colVideoId, "vector")

    //拆分
    for (i <- 0 to vectorDimension - 1) {
      df_levelOneBreak = df_levelOneBreak.withColumn("v_" + i, udfBreak(col("vector"), lit(i)))
    }


    df_levelOneBreak.drop("vector")
  }


  def getMeanVector(vectorDimension: Int, df_mediasPlayed: DataFrame, colName: String) = {
    /**
     * @description: get mean vector of colVideoTwoLevelClassification labels and colVideoTagList labels
     * @param: vectorDimension
     * @param: df_mediasPlayed:  medias video played by users
     * @param: colName : colVideoTwoLevelClassification, colVideoTagList
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    val df_videoListExplode = df_mediasPlayed.select(df_mediasPlayed(Dic.colVideoId),
      explode(df_mediasPlayed(colName))).toDF(Dic.colVideoId, "word")

    val df_mediaPart = df_mediasPlayed.select(Dic.colVideoId, colName)

    val df_labelVector = getVector(df_mediaPart, vectorDimension, colName)

    var df_videoIdWithVector = df_videoListExplode.join(df_labelVector, Seq("word"))
      .drop("word")

    //拆分
    for (i <- 0 to vectorDimension - 1) {
      df_videoIdWithVector = df_videoIdWithVector.withColumn("v_" + i, udfBreak(col("vector"), lit(i)))
    }

    df_videoIdWithVector.groupBy(Dic.colVideoId).mean()

  }

  def getVideoVector(vectorDimension: Int, df_levelOneVector: DataFrame, df_levelTwoMeanVector: DataFrame, df_tagsMeanVector: DataFrame, df_mediasPlayed: DataFrame) = {
    /**
     * @description: Union video's level one vector, level two vector and tags vector, get mean vector to be the result
     * @param: df_levelOneVector
     * @param: df_levelTwoMeanVector
     * @param: df_tagsMeanVector
     * @param: df_mediasPlayed : medias videos played by users
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    var df_videoLabelsVector = df_levelOneVector.union(df_levelTwoMeanVector).union(df_tagsMeanVector).groupBy(Dic.colVideoId).mean()


    for (i <- 0 to vectorDimension - 1) {
      df_videoLabelsVector = df_videoLabelsVector.withColumnRenamed("avg(v_" + i + ")", "v_" + i)

    }

    //It's  multiple columns of vector, not assemble yet, cause we need to process further in file TrainSetProcess
    df_videoLabelsVector
  }


}


