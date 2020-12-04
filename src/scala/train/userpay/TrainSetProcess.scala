package train.userpay

import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object TrainSetProcess {

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("TrainSetProcess")
      //.master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()


    val now = args(0) + " " + args(1)
    println(now)

    trainSetProcess(spark, now)


  }


  def trainSetProcess(spark: SparkSession, now: String) = {

    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""

    /**
     * Data Save Path
     */
    val trainUserProfileSavePath = hdfsPath + "data/train/userpay/trainUserProfile" + now.split(" ")(0)
    //    val trainSetSavePath = hdfsPath + "data/train/userpay/trainSet" + now.split(" ")(0)
    val trainSetSavePath = hdfsPath + "data/train/userpay/OrderAndUserProfile" + now.split(" ")(0)

    /**
     * User And Profile Data Path
     */
    val trainUsersPath = "hdfs:///pay_predict/data/train/userpay/trainUsers" + now.split(" ")(0)
    val userProfilePlayPartPath = hdfsPath + "data/train/common/processed/userpay/userprofileplaypart" + now.split(" ")(0)
    val userProfilePreferencePartPath = hdfsPath + "data/train/common/processed/userpay/userprofilepreferencepart" + now.split(" ")(0)
    val userProfileOrderPartPath = hdfsPath + "data/train/common/processed/userpay/userprofileorderpart" + now.split(" ")(0)

    /**
     * Medias Label Info Path
     */
    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt"


    /**
     * User History Data Path
     */
    val userPlayVectorPath = hdfsPath + "data/train/common/processed/userpay/history/playHistoryVector" + now.split(" ")(0)
    val orderHistoryPath = hdfsPath + "data/train/common/processed/userpay/history/orderHistory" + now.split(" ")(0)


    /**
     * Get Data
     */
    val df_user_profile_play = getData(spark, userProfilePlayPartPath)
    printDf("输入 df_user_profile_play", df_user_profile_play)

    val df_user_profile_pref = getData(spark, userProfilePreferencePartPath)
    printDf("输入 df_user_profile_pref", df_user_profile_pref)

    val df_user_profile_order = getData(spark, userProfileOrderPartPath)
    printDf("输入 df_user_profile_order", df_user_profile_order)

    val df_video_first_category = spark.read.format("csv").load(videoFirstCategoryTempPath)
    printDf("输入 df_video_first_category", df_video_first_category)

    val df_video_second_category = spark.read.format("csv").load(videoSecondCategoryTempPath)
    printDf("输入 df_video_second_category", df_video_second_category)

    val df_label = spark.read.format("csv").load(labelTempPath)
    printDf("输入 df_label", df_label)

    //    val df_play_vector = getData(spark, userPlayVectorPath)
    //    printDf("输入 df_play_vector", df_play_vector)

    val df_order_history = getData(spark, orderHistoryPath)
    printDf("输入 df_order_history", df_order_history)

    val df_train_users = getData(spark, trainUsersPath)
    printDf("输入 df_train_users", df_train_users)


    /**
     * Process User Profile Data
     */
    val joinKeysUserId = Seq(Dic.colUserId)
    val df_profile_play_pref = df_user_profile_play.join(df_user_profile_pref, joinKeysUserId, "left")
    val df_user_profile = df_profile_play_pref.join(df_user_profile_order, joinKeysUserId, "left")


    val colList = df_user_profile.columns.toList
    val colTypeList = df_user_profile.dtypes.toList
    val mapColList = ArrayBuffer[String]()
    for (elem <- colTypeList) {
      if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
        mapColList.append(elem._1)
      }
    }


    val numColList = colList.diff(mapColList)

    val df_userProfile_filled = df_user_profile
      .na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0, numColList)


    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()


    // 一级分类
    var conList = df_video_first_category.collect()
    for (elem <- conList) {
      val s = elem.toString()
      videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }
    //二级分类
    conList = df_video_second_category.collect()
    for (elem <- conList) {
      val s = elem.toString()
      videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }

    //标签
    conList = df_label.collect()
    for (elem <- conList) {
      val s = elem.toString()
      labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }


    val prefColumns = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

    var df_userProfile_split_pref1 = df_userProfile_filled

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
      df_userProfile_split_pref1 = df_userProfile_split_pref1.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
        .withColumn(elem + "_2", udfFillPreference(col(elem), lit(2)))
        .withColumn(elem + "_3", udfFillPreference(col(elem), lit(3)))
    }


    def udfFillPreferenceIndex = udf(fillPreferenceIndex _)

    def fillPreferenceIndex(prefer: String, mapLine: String) = {
      if (prefer == null) {
        null
      } else {
        var tempMap: Map[String, Int] = Map()
        val lineIterator1 = mapLine.split(",")
        lineIterator1.foreach(m => tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
        tempMap.get(prefer)
      }
    }

    var df_userProfile_split_pref2 = df_userProfile_split_pref1
    for (elem <- prefColumns) {
      if (elem.contains(Dic.colVideoOneLevelPreference)) {
        df_userProfile_split_pref2 = df_userProfile_split_pref2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
      } else {
        df_userProfile_split_pref2 = df_userProfile_split_pref2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
      }
    }

    var df_userProfile_split_pref3 = df_userProfile_split_pref2
    for (elem <- prefColumns) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        df_userProfile_split_pref3 = df_userProfile_split_pref3.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      } else {
        df_userProfile_split_pref3 = df_userProfile_split_pref3.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      }
    }


    val columnTypeList = df_userProfile_split_pref3.dtypes.toList
    val columnList = ArrayBuffer[String]()
    for (elem <- columnTypeList) {
      if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        columnList.append(elem._1)
      }
    }

    val df_train_user_profile = df_userProfile_split_pref3.select(columnList.map(df_userProfile_split_pref3.col(_)): _*)

    // Save Train User Profile
    //    saveProcessedData(df_trainUserProfile, trainUserProfileSavePath)

    /**
     * Train User Profile Merge with Order History And Play History
     */

    val df_train_set = df_train_users.join(df_train_user_profile, joinKeysUserId, "left")
      .join(df_order_history, joinKeysUserId, "left")
//      .join(df_play_vector, joinKeysUserId, "left")

    printDf("输出 df_train_set", df_train_set)
    //    saveProcessedData(df_train_set, trainSetSavePath)

    println("Train Set Process Done！")


  }


}
