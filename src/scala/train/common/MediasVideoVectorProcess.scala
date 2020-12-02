package train.common

import mam.Dic
import mam.Utils.{getData, printDf, saveProcessedData, udfBreak}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql
import org.apache.spark.sql.functions._
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
      .master("local[4]")
      .appName("MediasVideoLabelsVectorProcess")
      .getOrCreate()

    //val hdfsPath = "hdfs:///pay_predict/"
    val hdfsPath = ""
    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val mediasVectorSavePath = hdfsPath + "data/train/common/processed/userpay/mediasVector"

    /**
     * Get Data
     */
    val df_medias = getData(spark, mediasProcessedPath)
    val df_plays = getData(spark, playsProcessedPath)


    /**
     *  Process Medias Vector
     */
    val df_mediasVecMultiCol = mediasVectorProces(df_medias, df_plays)


    /**
     * Save Medias Vector v1 to HDFS
     */
    saveProcessedData(df_mediasVecMultiCol, mediasVectorSavePath)
    println("Media's video vector saved !!! ")

  }


  def mediasVectorProces(df_medias: DataFrame, df_plays: DataFrame) = {
    /**
     * @description: Get vector of Medias video which played by users
     * @param: df_medias :  medias dataframe
     * @param: df_plays : plays dataframe
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/12/2
     */

    // 选择有play记录的video embedding
    val df_playsVideoId = df_plays.select(Dic.colVideoId).distinct()
    val df_mediasPlayed = df_medias.join(df_playsVideoId, Seq(Dic.colVideoId), "inner")
    println("Medias' videos played by users: ", df_mediasPlayed.count())

    // Medias further process about Labels
    val df_mediasPlayedProcess = df_mediasPlayed
      .na.drop("all")
      .dropDuplicates(Dic.colVideoId)
      .na.fill(Map((Dic.colVideoOneLevelClassification, "其他")))
      .withColumn(Dic.colVideoTwoLevelClassificationList,
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList)).otherwise(Array("其他")))
      .withColumn(Dic.colVideoTagList, when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)).otherwise(Array("其他")))

    val df_mediasPlayedLabels = df_mediasPlayedProcess.select(Dic.colVideoId, Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList)


    /**
     * Get vectors for labels
     */
    val vectorDimension = 32
    val df_levelOneVector = getLevelOneVector(vectorDimension, df_mediasPlayedLabels)
    printDf("Level one classification vector", df_levelOneVector)


    val df_levelTwoMeanVector = getMeanVector(vectorDimension, df_mediasPlayedLabels, Dic.colVideoTwoLevelClassificationList)
    printDf("Level two classification mean vector", df_levelTwoMeanVector)


    val df_tagsMeanVector = getMeanVector(vectorDimension, df_mediasPlayedLabels, Dic.colVideoTagList)
    printDf("Tags mean vector", df_tagsMeanVector)


    /**
     * Get video vector (vectorDimension is 37)
     * video Vector include video hours and score, is_paid, in_package, Labels word2vector
     * storage_date_gap of train and predict time(add in TrainSet/PredictSet file)
     */

    val df_labelsVector = getMeanVectorOfLabels(vectorDimension, df_levelOneVector, df_levelTwoMeanVector, df_tagsMeanVector, df_mediasPlayedLabels)
    printDf("df_labelsVector", df_labelsVector)


    val df_mediasPlayedProcessFurther = df_mediasPlayedProcess
      .withColumn(Dic.colVideoHour, col(Dic.colVideoTime) / 3600)
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId).>(0), 1).otherwise(0))
      .withColumn(Dic.colScore, col(Dic.colScore) / 10)
      .na.fill(Map((Dic.colIsPaid, 0), (Dic.colInPackage, 0)))

    // colVideoOneLevelClassification will be used in TrainSetProcess
    val df_mediasPart = df_mediasPlayedProcessFurther.select(Dic.colVideoId, Dic.colScore, Dic.colIsPaid, Dic.colInPackage, Dic.colVideoHour, Dic.colStorageTime, Dic.colVideoOneLevelClassification)

    val df_mediasVecMultiCol = df_labelsVector.join(df_mediasPart, Seq(Dic.colVideoId), "left")

    printDf("Medias' Vector Multiple Columns", df_mediasVecMultiCol)


    df_mediasVecMultiCol


  }





  def getVector(df: DataFrame, vectorDimension: Int, colName: String) = {
    /**
     * @author wj
     * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @description 训练Word2vector模型，得到视频的嵌入向量
     */

    val w2vModel = new Word2Vec()
      .setInputCol(colName) //编码的列名
      .setOutputCol("label_vector") //输出的对应向量
      .setVectorSize(vectorDimension)
      .setMinCount(0)

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

  def getMeanVectorOfLabels(vectorDimension: Int, df_levelOneVector: DataFrame, df_levelTwoMeanVector: DataFrame, df_tagsMeanVector: DataFrame, df_mediasPlayed: DataFrame) = {
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
