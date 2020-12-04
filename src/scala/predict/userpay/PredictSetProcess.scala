package predict.userpay


import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object PredictSetProcess {


  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserProfileProcessUserpayPredict")
      //.master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()


    /**
     * Data path
     */
    //val hdfsPath=""
    val hdfsPath = "hdfs:///pay_predict/"

    // Predict users path
    val predictUserPath = hdfsPath + "data/predict/userpay/predictUsers" + args(0)
    // User profile path
    val userProfilePlayPartPath = hdfsPath + "data/predict/common/processed/userpay/userprofileplaypart" + args(0)
    val userProfilePreferencePartPath = hdfsPath + "data/predict/common/processed/userpay/userprofilepreferencepart" + args(0)
    val userProfileOrderPartPath = hdfsPath + "data/predict/common/processed/userpay/userprofileorderpart" + args(0)
    // Labels path
    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt"
    // History Path
    val orderHistoryPath = hdfsPath + "data/predict/common/processed/userpay/history/orderHistory" + args(0)
    //    val playVectorPath = hdfsPath + "data/predict/common/processed/userpay/history/playHistory" + args(0)

    /**
     * Train set save path
     */
    val predictSetSavePath = hdfsPath + "/data/predict/userpay/OrderHistoryUserProfile" + args(0)
    //     val predictSetSavePath = hdfsPath + "/data/predict/userpay/PredictSet" + args(0)
    /**
     * Get Data
     */
    // User Profile
    val df_user_profile_play = getData(spark, userProfilePlayPartPath)
    printDf("输入 df_user_profile_play", df_user_profile_play)

    val df_user_profile_pref = getData(spark, userProfilePreferencePartPath)
    printDf("输入 df_user_profile_pref", df_user_profile_pref)

    val df_user_profile_order = getData(spark, userProfileOrderPartPath)
    printDf("输入 df_user_profile_order", df_user_profile_order)
    // Predict Users
    val df_predict_users = getData(spark, predictUserPath)
    printDf("输入 df_predict_users", df_predict_users)
    // Labels
    val df_video_first_category = spark.read.format("csv").load(videoFirstCategoryTempPath)
    printDf("输入 df_video_first_category", df_video_first_category)

    val df_video_second_category = spark.read.format("csv").load(videoSecondCategoryTempPath)
    printDf("输入 df_video_second_category", df_video_second_category)

    val df_label = spark.read.format("csv").load(labelTempPath)
    printDf("输入 df_label", df_label)

    // History Data
    val df_order_history = getData(spark, orderHistoryPath)
    printDf("输入 df_order_history", df_order_history)

    //    val df_play_vector = getData(spark, playVectorPath)
    //    printDf("输入 df_play_vector", df_play_vector)


    // Get All User Profile Data
    val joinKeysUserId = Seq(Dic.colUserId)
    val df_play_join_pref = df_user_profile_play.join(df_user_profile_pref, joinKeysUserId, "left")
    val df_user_profile = df_play_join_pref.join(df_user_profile_order, joinKeysUserId, "left")
    val df_predict_profile = df_predict_users.join(df_user_profile, joinKeysUserId, "left")

    // Fill Null Value
    val colList = df_predict_profile.columns.toList
    val colTypeList = df_predict_profile.dtypes.toList
    val mapColList = ArrayBuffer[String]()

    for (elem <- colTypeList) {
      if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
        mapColList.append(elem._1)
      }
    }
    val numColList = colList.diff(mapColList)
    val df_tempPredictSet = df_predict_profile.na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
    val df_predict_set_not_null = df_tempPredictSet.na.fill(0, numColList)

    /**
     * Split Pref Data
     */
    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()


    var conList = df_video_first_category.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }

    conList = df_video_second_category.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }

    conList = df_label.collect()
    for (elem <- conList) {
      var s = elem.toString()
      labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }


    val prefColumns = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

    var df_temp_df1 = df_predict_set_not_null

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

    for (elem <- prefColumns) {
      df_temp_df1 = df_temp_df1.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
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

    var df_temp_df2 = df_temp_df1

    for (elem <- prefColumns) {
      if (elem.contains(Dic.colVideoOneLevelPreference)) {
        df_temp_df2 = df_temp_df2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
      } else {
        df_temp_df2 = df_temp_df2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
      }
    }

    var df_temp_df3 = df_temp_df2
    for (elem <- prefColumns) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        df_temp_df3 = df_temp_df3.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      } else {
        df_temp_df3 = df_temp_df3.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      }

    }
    //df_tempDataFrame.filter(col("video_one_level_preference_1")=!=13).show()
    val columnTypeList = df_temp_df3.dtypes.toList
    val columnList = ArrayBuffer[String]()
    for (elem <- columnTypeList) {
      if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        columnList.append(elem._1)
      }
    }

    val df_predict_user_profile = df_temp_df3.select(columnList.map(df_temp_df3.col(_)): _*)


    /**
     * Predict User Profile Merge with Order History And Play History to be Train Set
     */


    val df_predict_set = df_predict_user_profile
      .join(df_order_history, joinKeysUserId, "left")
    //    .join(df_play_vector, joinKeysUserId, "left")

    printDf("输出 df_predict_set", df_predict_set)
    //    saveProcessedData(df_predict_set, predictSetSavePath)

    println("Predict Set Save Done！")


  }

}
