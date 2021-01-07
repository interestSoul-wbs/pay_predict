package train.userpay

import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import mam.Utils._
import mam.GetSaveData._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, isnull, udf}

object FeatureProcessOld {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    //最初生成的用户画像数据集路径
    val df_user_profile_play_part = getUserProfilePlayPart(partitiondate, license, "train")

    val df_user_profile_preference_part = getuserProfilePreferencePart(partitiondate, license, "train")

    val df_user_profile_order_part = getUserProfileOrderPart(partitiondate, license, "train")

    val joinKeysUserId = Seq(Dic.colUserId)

    val userProfiles = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    val df_user_list = getTrainUser(partitiondate, license, "train", "old")

    val trainSet = df_user_list.join(userProfiles, joinKeysUserId, "left")

    val colList = trainSet.columns.toList

    val colTypeList = trainSet.dtypes.toList

    val mapColList = ArrayBuffer[String]()

    for (elem <- colTypeList) {
      if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
        mapColList.append(elem._1)
      }
    }

    val numColList = colList.diff(mapColList)

    val tempTrainSet = trainSet.na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))

    val trainSetNotNull = tempTrainSet.na.fill(0, numColList)

    //# 观看时长异常数据处理：1天24h
    val df_video_first_category = getVideoCategory(partitiondate, license, "one_level")

    val df_video_second_category = getVideoCategory(partitiondate, license, "two_level")

    val videoFirstCategoryMap = getCategoryMap(df_video_first_category)

    val videoSecondCategoryMap = getCategoryMap(df_video_second_category)

    val pre = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

    var tempDataFrame = trainSetNotNull

    printDf("tempDataFrame_1", tempDataFrame)

    for (elem <- pre) {
      tempDataFrame = tempDataFrame.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
        .withColumn(elem + "_2", udfFillPreference(col(elem), lit(2)))
        .withColumn(elem + "_3", udfFillPreference(col(elem), lit(3)))
    }

    printDf("tempDataFrame_2", tempDataFrame)

    for (elem <- pre) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        tempDataFrame = tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
      } else {
        tempDataFrame = tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
      }
    }

    printDf("tempDataFrame_3", tempDataFrame)

    for (elem <- pre) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        tempDataFrame = tempDataFrame.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      } else {
        tempDataFrame = tempDataFrame.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      }
    }

    printDf("tempDataFrame_4", tempDataFrame)

    val columnTypeList = tempDataFrame.dtypes.toList
    val columnList = ArrayBuffer[String]()
    for (elem <- columnTypeList) {
      if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        columnList.append(elem._1)
      }
    }

    val df_result = tempDataFrame.select(columnList.map(tempDataFrame.col(_)): _*)

    printDf("df_result", df_result)

    saveFeatureProcessResult(df_result, partitiondate, license, "train", "old")
  }

}
