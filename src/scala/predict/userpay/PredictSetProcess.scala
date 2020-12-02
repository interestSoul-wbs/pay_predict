package predict.userpay


import mam.Dic
import mam.Utils.{getData, mapIdToMediasVector, printDf, saveProcessedData, udfGetDays, udfLog, udfWeightVector}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit, mean, when}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}


object PredictSetProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      //.master("local[4]")
      .appName("TrainSetProcess")
      .getOrCreate()


    val trainTime = args(0) + " " + args(1)
    println(trainTime)


    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""

    val mediasVideoVectorPath = hdfsPath + "data/train/common/processed/userpay/mediasVector"

    val playHistoryPath = hdfsPath + "data/train/common/processed/userpay/history/playHistory" + args(0)
    val orderHistoryPath = hdfsPath + "data/train/common/processed/userpay/history/orderHistory" + args(0)
    val videoPlayedTimesPath = hdfsPath + "data/train/common/processed/userpay/history/videoInPackagePlayTimes"

    val trainUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)
    val trainUserProfileSavePath = hdfsPath + "data/train/userpay/trainUserProfile" + args(0)

    val packageExpByVideoSavePath = hdfsPath + "data/train/userpay/packageExpByVideoSavePath" + args(0)
    val trainSetSavePath = hdfsPath + "data/train/userpay/trainSet" + args(0)

    val playHistoryVectorSavePath = hdfsPath + "data/train/common/processed/userpay/history/playHistoryVector" + args(0)

    /**
     * Get processed user profile and merge with train users dataframe
     */

    // Get train users
    val df_trainUsers = getData(spark, trainUsersPath)
    val df_trainId = df_trainUsers.select(Dic.colUserId)

    // Get users' play history
    var df_playHistory = getData(spark, playHistoryPath)
    printDf("df_playHistory", df_playHistory)

    var df_trainUserPlayHistory = df_trainId.join(df_playHistory, Seq(Dic.colUserId), "inner")
    printDf("df_trainPartUserPlayHistory", df_trainUserPlayHistory)


    /**
     * Medias vector add storage time gap info
     * Assemble all columns of medias to be video vector
     * videoVectorDimension is 37
     */

    val df_mediasVectorPart = getData(spark, mediasVideoVectorPath)
    printDf("df_mediasVectorPart", df_mediasVectorPart)

    val df_videoVector = mediasVectorProcess(df_mediasVectorPart, trainTime)
    printDf("df_videoVector", df_videoVector)


    //Map train users' video id list to medias' video vector
    val df_trainUserPlayHistoryVector = mapVideoVector(df_trainUserPlayHistory, df_videoVector, topNPlayHistory = 50)

    saveProcessedData(df_trainUserPlayHistoryVector.limit(100), playHistoryVectorSavePath)

    println("Train Users' Play History Vector Save Done...")

    // Get users' order history
    val df_orderHistory = getData(spark, orderHistoryPath)
    printDf("df_orderHistory", df_orderHistory)

    val df_trainUserOrderHistory = df_trainUsers.join(df_orderHistory, Seq(Dic.colUserId), "left")
    printDf("df_trainUserOrderHistory", df_trainUserOrderHistory)


    //User Profile
    val df_trainUserProfile = getData(spark, trainUserProfileSavePath)
    printDf("df_trainUserProfile", df_trainUserProfile)

    val df_trainSet = df_trainUserOrderHistory.join(df_trainUserProfile, Seq(Dic.colUserId), "left")
    printDf("df_trainSet", df_trainSet)

    /**
     * Save data
     */

    saveProcessedData(df_trainSet, trainSetSavePath)
    println("Train Users Left Join with Order History and User Profile Data Save Done!")


    /**
     * Use videos' vector in package to express package
     */
    //   Video played times
    val df_playVideoTimes = getData(spark, videoPlayedTimesPath) // Video in package 100201 and 100202
    var df_packageExpByVideo = df_videoVector.join(df_playVideoTimes, Seq(Dic.colVideoId), "inner") //video vector is played video


    // Video vector and play times weighted
    df_packageExpByVideo = df_packageExpByVideo.withColumn("log_count", when(col(Dic.colPlayTimes) > 0, udfLog(col(Dic.colPlayTimes))).otherwise(0))
      .withColumn("vector", col("vector").cast(StringType))
      .withColumn("weighted_vector", udfWeightVector(col("vector"), col("log_count")))
      .select(Dic.colVideoId, "weighted_vector")

    printDf("df_packageExpByVideo", df_packageExpByVideo)


    //    Use python to select "weighted_vector" to be a Matrix then merge with every user
    saveProcessedData(df_packageExpByVideo, packageExpByVideoSavePath)
    println("packageExpByVideo dataframe save done!")


  }


  def mediasVectorProcess(df_mediasVectorPart: DataFrame, trainTime: String) = {
    /**
     * @description: Add storage time gap to medias info then assemble to video vector
     * @param: df_mediasVectorPart : medias dataframe
     * @param: trainTime
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/30
     */

    val df_mediasVector = df_mediasVectorPart.withColumn("trainTime", lit(trainTime))
      .withColumn(Dic.colStorageTimeGap, udfGetDays(col(Dic.colStorageTime), col("trainTime")))
      .drop("trainTime", Dic.colStorageTime)

    // fill na colStorageTime with colLevelOne mean storage time
    val df_fillGapMedias = fillStorageGap(df_mediasVector)

    printDf("df_mediasVector", df_fillGapMedias)

    // Concat columns to be vector of the videos
    val mergeCols = df_fillGapMedias.columns.filter(!_.contains(Dic.colVideoId)) //remove column videoId
    val assembler = new VectorAssembler()
      .setInputCols(mergeCols)
      .setHandleInvalid("keep")
      .setOutputCol("vector")

    assembler.transform(df_fillGapMedias).select(Dic.colVideoId, "vector")

  }


  def fillStorageGap(df_medias: DataFrame): DataFrame = {
    /**
     * @describe 根据video的视频一级分类进行相关列空值的填充
     * @author wx
     * @param [mediasDf]
     * @param [spark]
     * @return {@link DataFrame }
     * */
    val df_mean = df_medias.groupBy(Dic.colVideoOneLevelClassification).agg(mean(col(Dic.colStorageTimeGap)))
      .withColumnRenamed("avg(" + Dic.colStorageTimeGap + ")", "mean_" + Dic.colStorageTimeGap)

    printDf("medias中一级分类的" + Dic.colStorageTimeGap + "平均值", df_mean)

    // video的colName全部video平均值
    val meanValue = df_medias.agg(mean(Dic.colStorageTimeGap)).collectAsList().get(0).get(0)
    println("mean " + Dic.colStorageTimeGap, meanValue)


    val df_mediasJoinMean = df_medias.join(df_mean, Seq(Dic.colVideoOneLevelClassification), "inner")
    printDf("df_mediasJoinMean", df_mediasJoinMean)


    val df_meanFilled = df_mediasJoinMean.withColumn(Dic.colStorageTimeGap, when(col(Dic.colStorageTimeGap).>=(0.0), col(Dic.colStorageTimeGap))
      .otherwise(col("mean_" + Dic.colStorageTimeGap)))
      .na.fill(Map((Dic.colStorageTimeGap, meanValue)))
      .drop("mean_" + Dic.colStorageTimeGap)

    df_meanFilled.withColumn(Dic.colStorageTimeGap, udfLog(col(Dic.colStorageTimeGap)))
      .drop(Dic.colVideoOneLevelClassification)
  }

  def mapVideoVector(df_trainUserPlayHistory: DataFrame, df_videoVector: DataFrame, topNPlayHistory: Int) = {
    /**
     * @description: Map the video id list to vector
     * @param:df_playHistory : df_playHistory Dataframe which has video id list
     * @param: df_videoVector  : Video Vector Dataframe, (columns video_id, vector)
     * @param: topNPlayHistory : play history video's number
     * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @author: wx
     * @Date: 2020/11/26
     */

    import scala.collection.mutable

    /**
     * Medias to Map( video_id -> vector)
     */
    // medias map( id-> vector )
    val mediasMap = df_videoVector.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoId).toString -> row.getAs("vector").toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    println("Medias Map size", mediasMap.size)

    printDf("df_trainUserPlayHistory", df_trainUserPlayHistory)

    val df_playVector = df_trainUserPlayHistory.withColumn("play_vector", mapIdToMediasVector(mediasMap)(col(Dic.colVideoId + "_list")))
      .drop(Dic.colVideoId + "_list")


    printDf("df_playVector", df_playVector)

    df_playVector

  }


}
