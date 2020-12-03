package train.common

import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{getData, printDf, udfBreak}
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

  val vectorDimension = 32

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
    printDf("df_medias", df_medias)

    val df_plays = getData(spark, playsProcessedPath)
    printDf("df_plays", df_plays)


    /**
     * Process Medias Vector
     */
    val df_medias_vector_v1 = mediasVectorProces(df_medias, df_plays)
    saveProcessedData(df_medias_vector_v1, mediasVectorSavePath)
    printDf("df_medias_vector_v1", df_medias_vector_v1)
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
    val df_played_video = df_plays.select(Dic.colVideoId).distinct()
    val df_medias_played = df_medias.join(df_played_video, Seq(Dic.colVideoId), "inner")
    println("Medias' videos played by users: ", df_medias_played.count())

    // Medias further process about Labels
    val df_medias_played_process = df_medias_played
      .na.drop("all")
      .dropDuplicates(Dic.colVideoId)
      .withColumn(Dic.colVideoTwoLevelClassificationList,
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList))
          .otherwise(Array("其他")))
      .withColumn(Dic.colVideoTagList,
        when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList))
          .otherwise(Array("其他")))
      .withColumn(Dic.colVideoHour, col(Dic.colVideoTime) / 3600)
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId).>(0), 1).otherwise(0))
      .withColumn(Dic.colScore, col(Dic.colScore) / 10)
      .na.fill(
      Map((Dic.colIsPaid, 0),
        (Dic.colInPackage, 0),
        (Dic.colVideoOneLevelClassification, "其他"))
    )

    val df_medias_played_labels = df_medias_played_process.select(Dic.colVideoId, Dic.colVideoOneLevelClassification, Dic.colVideoTwoLevelClassificationList, Dic.colVideoTagList)


    /**
     * Get video vector (vectorDimension is 37)
     * video Vector include video hours and score, is_paid, in_package, Labels word2vector
     * storage_date_gap of train and predict time(add in TrainSet/PredictSet file)
     */

    val df_one_vector = getLevelOneVector(vectorDimension, df_medias_played_labels)
    printDf("Level one classification vector", df_one_vector)

    val df_two_mean_vector = getMeanVector(vectorDimension, df_medias_played_labels, Dic.colVideoTwoLevelClassificationList)
    printDf("Level two classification mean vector", df_two_mean_vector)

    val df_tags_mean_vector = getMeanVector(vectorDimension, df_medias_played_labels, Dic.colVideoTagList)
    printDf("Tags mean vector", df_tags_mean_vector)

    val df_labels_vector = getMeanVectorOfLabels(vectorDimension, df_one_vector, df_two_mean_vector, df_tags_mean_vector, df_medias_played_labels)
    printDf("df_labelsVector", df_labels_vector)



    // colVideoOneLevelClassification will be used in TrainSetProcess
    val df_medias_part = df_medias_played_process.select(Dic.colVideoId, Dic.colScore, Dic.colIsPaid, Dic.colInPackage, Dic.colVideoHour, Dic.colStorageTime, Dic.colVideoOneLevelClassification)

    val df_medias_vector_v1 = df_labels_vector.join(df_medias_part, Seq(Dic.colVideoId), "left")

    printDf("Medias' Vector Multiple Columns", df_medias_vector_v1)


    df_medias_vector_v1


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


  def getLevelOneVector(vectorDimension: Int, df_medias_played: DataFrame) = {
    /**
     * @description: get vectors of video's colVideoOneLevelClassification
     * @param: vectorDimension
     * @param: df_medias_played : medias videos played by users
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    // In function getVector fit() need to br Array[String] type
    val df_level_one = df_medias_played
      .groupBy(col(Dic.colVideoId))
      .agg(collect_list(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification + "_list"))

    val df_level_one_vector = getVector(df_level_one, vectorDimension, Dic.colVideoOneLevelClassification + "_list")
      .withColumnRenamed("word", Dic.colVideoOneLevelClassification)
    printDf("get level one vector", df_level_one_vector)

    //medias dataframe join with level one vector
    var df_level_one_break = df_medias_played.join(df_level_one_vector, Seq(Dic.colVideoOneLevelClassification), "inner")
      .select(Dic.colVideoId, "vector")

    //拆分
    for (i <- 0 to vectorDimension - 1) {
      df_level_one_break = df_level_one_break.withColumn("v_" + i, udfBreak(col("vector"), lit(i)))
    }


    df_level_one_break.drop("vector")
  }


  def getMeanVector(vectorDimension: Int, df_medias_played: DataFrame, colName: String) = {
    /**
     * @description: get mean vector of colVideoTwoLevelClassification labels and colVideoTagList labels
     * @param: vectorDimension
     * @param: df_medias_played:  medias video played by users
     * @param: colName : colVideoTwoLevelClassification, colVideoTagList
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    val df_video_list_explode = df_medias_played.select(df_medias_played(Dic.colVideoId),
      explode(df_medias_played(colName))).toDF(Dic.colVideoId, "word")

    val df_media_part = df_medias_played.select(Dic.colVideoId, colName)

    val df_labelVector = getVector(df_media_part, vectorDimension, colName)

    var df_video_id_and_Vector = df_video_list_explode.join(df_labelVector, Seq("word"))
      .drop("word")

    //拆分
    for (i <- 0 to vectorDimension - 1) {
      df_video_id_and_Vector = df_video_id_and_Vector.withColumn("v_" + i, udfBreak(col("vector"), lit(i)))
    }

    df_video_id_and_Vector.groupBy(Dic.colVideoId).mean()

  }

  def getMeanVectorOfLabels(vectorDimension: Int, df_one_vector: DataFrame, df_two_mean_vector: DataFrame, df_tagsMeanVector: DataFrame, df_medias_played: DataFrame) = {
    /**
     * @description: Union video's level one vector, level two vector and tags vector, get mean vector to be the result
     * @param: df_one_vector
     * @param: df_two_mean_vector
     * @param: df_tagsMeanVector
     * @param: df_medias_played : medias videos played by users
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/25
     */

    var df_labels_vector = df_one_vector.union(df_two_mean_vector).union(df_tagsMeanVector).groupBy(Dic.colVideoId).mean()

    for (i <- 0 to vectorDimension - 1) {
      df_labels_vector = df_labels_vector.withColumnRenamed("avg(v_" + i + ")", "v_" + i)

    }
    //It's  multiple columns of vector, not assemble yet, cause we need to process further in file TrainSetProcess
    df_labels_vector
  }


}
