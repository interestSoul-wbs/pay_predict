package predict.userpay


import mam.Dic
import mam.Utils.{getData, printDf, saveProcessedData}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object PredictSetProcess {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileProcessUserpayPredict")
      //.master("local[6]")
      .getOrCreate()

    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath=""

    //Predict Users
    val predictUserPath = hdfsPath + "data/predict/userpay/predictUsers" + args(0)

    //训练集数据的保存路径
    val predictUserProfileSavePath = hdfsPath + "data/predict/userpay/predictUserProfile" + args(0)

    //最初生成的用户画像数据集路径
    val userProfilePlayPartPath = hdfsPath + "data/predict/common/processed/userpay/userprofileplaypart" + args(0)
    val userProfilePreferencePartPath = hdfsPath + "data/predict/common/processed/userpay/userprofilepreferencepart" + args(0)
    val userProfileOrderPartPath = hdfsPath + "data/predict/common/processed/userpay/userprofileorderpart" + args(0)


    val df_userProfilePlayPart = getData(spark, userProfilePlayPartPath)
    val df_userProfilePreferencePart = getData(spark, userProfilePreferencePartPath)
    val df_userProfileOrderPart = getData(spark, userProfileOrderPartPath)

    val joinKeysUserId = Seq(Dic.colUserId)
    val df_userProfilePlayAndPref = df_userProfilePlayPart.join(df_userProfilePreferencePart, joinKeysUserId, "left")
    val df_userProfiles = df_userProfilePlayAndPref.join(df_userProfileOrderPart, joinKeysUserId, "left")

    val df_predictUsers = getData(spark, predictUserPath)

    val df_predictProfile = df_predictUsers.join(df_userProfiles, joinKeysUserId, "left")

    val colList = df_predictProfile.columns.toList
    val colTypeList = df_predictProfile.dtypes.toList
    val mapColList = ArrayBuffer[String]()
    for (elem <- colTypeList) {
      if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
        mapColList.append(elem._1)
      }
    }
    val numColList = colList.diff(mapColList)
    val df_tempPredictSet = df_predictProfile.na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
    val df_predictSetNotNull = df_tempPredictSet.na.fill(0, numColList)


    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt"

    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()


    var videoFirstCategory = spark.read.format("csv").load(videoFirstCategoryTempPath)
    var conList = videoFirstCategory.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }

    var videoSecondCategory = spark.read.format("csv").load(videoSecondCategoryTempPath)
    conList = videoSecondCategory.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }

    var label = spark.read.format("csv").load(labelTempPath)
    conList = label.collect()
    for (elem <- conList) {
      var s = elem.toString()
      labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }


    val pre = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

    var df_tempDataFrame = df_predictSetNotNull

    def udfFillPreference = udf(fillPreference _)

    def fillPreference(prefer: Map[String, Int], offset: Int) = {
      if (prefer == null) {
        null
      } else {
        val mapArray = prefer.toArray
        if (mapArray.length > offset - 1) {
          mapArray(offset - 1)._1
        } else {
          null
        }

      }

    }

    for (elem <- pre) {
      df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
        .withColumn(elem + "_2", udfFillPreference(col(elem), lit(2)))
        .withColumn(elem + "_3", udfFillPreference(col(elem), lit(3)))
    }

    def udfFillPreferenceIndex = udf(fillPreferenceIndex _)

    def fillPreferenceIndex(prefer: String, mapLine: String) = {
      if (prefer == null) {
        null
      } else {
        var tempMap: Map[String, Int] = Map()
        var lineIterator1 = mapLine.split(",")
        //迭代打印所有行
        lineIterator1.foreach(m => tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
        tempMap.get(prefer)
      }
    }

    for (elem <- pre) {
      if (elem.contains(Dic.colVideoOneLevelPreference)) {
        df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
      } else {
        df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
      }
    }
    //df_tempDataFrame.filter(!isnull(col("video_one_level_preference_1"))).show()
    for (elem <- pre) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        df_tempDataFrame = df_tempDataFrame.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      } else {
        df_tempDataFrame = df_tempDataFrame.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      }

    }
    //df_tempDataFrame.filter(col("video_one_level_preference_1")=!=13).show()
    val columnTypeList = df_tempDataFrame.dtypes.toList
    val columnList = ArrayBuffer[String]()
    for (elem <- columnTypeList) {
      if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        columnList.append(elem._1)
      }
    }

    val df_predictUserProfile = df_tempDataFrame.select(columnList.map(df_tempDataFrame.col(_)): _*)

//    saveProcessedData(df_predictUserProfile, predictUserProfileSavePath)


    /**
     * Train User Profile Merge with Order History And Play History to be Train Set
     */
    val orderHistoryPath = hdfsPath + "data/predict/common/processed/userpay/history/orderHistory" + args(0)
    val df_orderHistory = getData(spark, orderHistoryPath)

    //    val playVectorPath = hdfsPath + "data/predict/common/processed/userpay/history/playHistory" + args(0)
    //    val df_playHistory = getData(spark, playHistoryPath)

    val predictSetPath = hdfsPath + "/data/predict/userpay/OrderHistoryUserProfile" + args(0)
    val df_userProfileAndOrderHistory = df_predictUserProfile.join(df_orderHistory, joinKeysUserId, "left")
    saveProcessedData(df_userProfileAndOrderHistory, predictSetPath)

    //    val df_trainSet = df_userProfileAndOrderHistory.join(df_playVector, joinKeysUserId, "left")
    //
    //    saveProcessedData(df_trainSet, trainSetSavePath)
    println("Done！")


  }

}
