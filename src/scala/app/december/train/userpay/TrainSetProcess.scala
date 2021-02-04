package app.december.train.userpay

import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import rs.common.DateTimeTool._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import rs.common.SparkSessionInit
import rs.common.SparkSessionInit.spark

import scala.collection.mutable.ArrayBuffer

object TrainSetProcess {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var category: String = _
  var processedMediasData: String = _
  var realSysDate: String = _
  var realSysDateOneDayAgo: String = _
  val timeWindow = 30

  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // 2020-12-1 - union1.x
    sector = args(3).toInt // 用户分群后的群代号
    category = args(4)
    processedMediasData = args(5)
    realSysDate = getRealSysDateTimeString
    realSysDateOneDayAgo = getDaysAgoAfter(realSysDate, -1)

    val trainTime = partitiondateToStandard(partitiondate)
    println("trainTime", trainTime)

    // 2 Get Data
    val df_user_profile_play = getUserProfilePlayPartV2(partitiondate, license, vodVersion, sector, category)

    val df_user_profile_pref = getUserProfilePreferencePartV2(partitiondate, license, vodVersion, sector, category)

    val df_user_profile_order = getUserProfileOrderPartV2(partitiondate, license, vodVersion, sector, category)

    //
    val df_video_first_category = getVideoCategory(processedMediasData, license, Dic.colOneLevel)

    val df_video_second_category = getVideoCategory(processedMediasData, license, Dic.colTwoLevel)

    val df_label = getVideoCategory(processedMediasData, license, Dic.colVideoTag)

    val df_train_user = getUserSplitResultV2(partitiondate, license, vodVersion, sector, category)

    // 3 Train Set Process
    val df_train_set = dataSetProcess(df_user_profile_play, df_user_profile_pref, df_user_profile_order,
      df_video_first_category, df_video_second_category, df_label, df_train_user)

    // 4 Save Train Users
    saveUserpayFianlDataV2(df_train_set, partitiondate, license, vodVersion, sector, category)
  }


  def dataSetProcess(df_user_profile_play: DataFrame, df_user_profile_pref: DataFrame, df_user_profile_order: DataFrame,
                      df_video_first_category: DataFrame, df_video_second_category: DataFrame, df_label: DataFrame, df_train_user: DataFrame): DataFrame = {

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

    /**
      * 将添加用户的标签信息
      */
    val df_train_user_profile = df_userProfile_split_pref3.select(columnList.map(df_userProfile_split_pref3.col(_)): _*)
    val df_train_set = df_train_user.join(df_train_user_profile, joinKeysUserId, "left")

    df_train_set
  }

}
