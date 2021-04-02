package train.userpay

import mam.GetSaveData.{getProcessedMedias, getProcessedOrder, getProcessedPlay, getVideoFirstCategory, getVideoLabel, getVideoSecondCategory, hdfsPath, saveCSVFile, saveProcessedData, saveUserProfileOrderPart, saveUserProfilePlayPart, saveUserProfilePreferencePart}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}
import train.userpay.UserProfileGenerateOrderPartForUserpay.userProfileGenerateOrderPart
import train.userpay.UserProfileGeneratePlayPartForUserpay.userProfileGeneratePlayPart
import train.userpay.UserProfileGeneratePreferencePartForUserpay.userProfileGeneratePreferencePart

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object hzy {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    val playMin = "2020-09-02 00:00:00"
    val playMax = "2020-10-08 00:00:00"


    val df_play = getProcessedPlay(spark)
    printDf("输入 df_play", df_play)


    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)

    // play

    val train_user_path = hdfsPath + "data/hzy/train/trainplayusers.txt"
    val df_train_play_users = spark.read.format("csv").load(train_user_path).as(Dic.colUserId)
    printDf("df_train_play_users", df_train_play_users)


    val test_user_path = hdfsPath + "data/hzy/predict/testplayusers.txt"
    val df_test_play_users = spark.read.format("csv").load(test_user_path).as(Dic.colUserId)
    printDf("df_test_play_users", df_test_play_users)


    val df_train_play = df_play
      .join(df_train_play_users, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colPlayStartTime) >= playMin
          && col(Dic.colPlayStartTime) < playMax
      )

    printDf("输出 df_train_play", df_train_play)

    val train_play_path = "data/hzy/train/trainPlayUsersPlay.csv"
    saveCSVFile(df_train_play, train_play_path)


    val df_test_play = df_play
      .join(df_test_play_users, Seq(Dic.colUserId), "inner")
      .filter(
        col(Dic.colPlayStartTime) >= playMin
          && col(Dic.colPlayStartTime) < playMax
      )
    printDf("输出 df_test_play", df_test_play)

    val test_play_path = hdfsPath + "data/hzy/predict/predictPlayUsersPlay.csv"
    saveCSVFile(df_test_play, test_play_path)




    // UserProfile

    val trainTime = "2020-10-01 00:00:00"
    val predictTime = "2020-10-08 00:00:00"


    val df_video_first_category = getVideoFirstCategory()
    printDf("输入 df_video_first_category", df_video_first_category)

    val df_video_second_category = getVideoSecondCategory()
    printDf("输入 df_video_second_category", df_video_second_category)

    val df_label = getVideoLabel()
    printDf("输入 df_label", df_label)


    val df_train_users = getTrainUserForHzy()
    printDf("输出 df_train_users", df_train_users)

    val df_predict_users = getPredictUserForHzy()
    printDf("输出 df_predict_users", df_predict_users)


    val df_train_set = getDataSetForHzy(df_train_users, trainTime, df_medias, df_play, df_orders,
      df_video_first_category, df_video_second_category, df_label)

    saveCSVFile(df_train_set, hdfsPath + "data/hzy/train/trainDataSet.csv")
    printDf("输出 df_train_set", df_train_set)


    val df_predict_set = getDataSetForHzy(df_predict_users, predictTime, df_medias, df_play, df_orders,
      df_video_first_category, df_video_second_category, df_label)

    saveCSVFile(df_predict_set, hdfsPath + "data/hzy/predict/predictDataSet.csv")

    printDf("输出 df_predict_set", df_predict_set)


  }






  def getTrainUserForHzy() = {
    val path = hdfsPath + "data/hzy/train/trainusers.txt"
    spark.read.format("csv").load(path).as(Dic.colUserId)

  }

  def getPredictUserForHzy() = {
    val path = hdfsPath + "data/hzy/train/trainusers.txt"
    spark.read.format("csv").load(path).as(Dic.colUserId)
  }

  def getDataSetForHzy(df_user: DataFrame, now: String, df_medias: DataFrame, df_plays: DataFrame, df_orders: DataFrame,
                       df_video_first_category: DataFrame, df_video_second_category: DataFrame, df_label: DataFrame) = {



    // order
    val df_user_profile_order = userProfileGenerateOrderPart(spark, now, df_orders, df_user)

    // 4 Save Data
    saveUserProfileOrderPart(now, df_user_profile_order,"hzy")
    printDf("输出  df_user_profile_order", df_user_profile_order)

    println("用户画像订单部分生成完毕。")


    // pref

    val df_user_profile_pref = userProfileGeneratePreferencePart(now, df_plays, df_user, df_medias)

    // 4 Save Data
    saveUserProfilePreferencePart(now, df_user_profile_pref, "hzy")
    printDf("输出 df_user_profile_pref", df_user_profile_pref)

    println("用户画像Preference部分生成完毕。")


    // play

    val df_user_profile_play = userProfileGeneratePlayPart(now, df_plays, df_user, df_medias)

    // 4 Save Data
    saveUserProfilePlayPart(now, df_user_profile_play, "hzy")
    printDf("输出 df_user_profile_play", df_user_profile_play)

    println("用户画像play部分生成完毕。")

    // dataSet
    val df_dataSet = dataSetProcess(df_user_profile_play, df_user_profile_pref, df_user_profile_order,
      df_video_first_category, df_video_second_category, df_label, df_user)

    println(now + "数据集生成完成。")
    df_dataSet

  }


  def dataSetProcess(df_user_profile_play: DataFrame, df_user_profile_pref: DataFrame, df_user_profile_order: DataFrame,
                      df_video_first_category: DataFrame, df_video_second_category: DataFrame, df_label: DataFrame,
                      df_train_user: DataFrame): DataFrame = {


    /**
     * Process User Profile Data
     */
    val joinKeysUserId = Seq(Dic.colUserId)
    val df_profile_play_pref = df_user_profile_play.join(df_user_profile_pref, joinKeysUserId, "left")
    val df_user_profile = df_profile_play_pref.join(df_user_profile_order, joinKeysUserId, "left")


    /**
     * 空值填充
     */
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

    /**
     * 偏好部分标签处理
     */

    // 一级标签
    val videoFirstCategoryMap = df_video_first_category.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoOneLevelClassification).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, Int]]

    // 二级标签
    val videoSecondCategoryMap = df_video_second_category.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoTwoLevelClassificationList).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, Int]]


    // 类别型标签
    val labelMap = df_label.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoTagList).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, Int]]



    //    var videoFirstCategoryMap: Map[String, Int] = Map()
    //    var videoSecondCategoryMap: Map[String, Int] = Map()
    //    var labelMap: Map[String, Int] = Map()

    //    // 一级分类
    //    var conList = df_video_first_category.collect()
    //    for (elem <- conList) {
    //      val s = elem.toString()
    //      videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)
    //
    //    }
    //    //二级分类
    //    conList = df_video_second_category.collect()
    //    for (elem <- conList) {
    //      val s = elem.toString()
    //      videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)
    //
    //    }
    //
    //    //标签
    //    conList = df_label.collect()
    //    for (elem <- conList) {
    //      val s = elem.toString()
    //      labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)
    //
    //    }


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



    val df_train_user_prof = df_userProfile_split_pref3.select(columnList.map(df_userProfile_split_pref3.col(_)): _*)


    /**
     * 添加用户标签
     */

    val df_train_set = df_train_user.join(df_train_user_prof, joinKeysUserId, "left")
    df_train_set


  }




}
