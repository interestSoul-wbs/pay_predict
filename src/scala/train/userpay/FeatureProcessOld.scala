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
    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "train")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "train")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "train")

    val joinKeysUserId = Seq(Dic.colUserId)

    val userProfiles = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    val df_user_list = getTrainUser(spark, partitiondate, license, "train", "old")

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
    val df_video_first_category = getVideoCategory(spark, partitiondate, license, "one_level")

    val df_video_second_category = getVideoCategory(spark, partitiondate, license, "two_level")

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

    saveTrainSet(spark, df_result, partitiondate, license, "train", "old")
  }

  def saveTrainSet(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String, new_or_old: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_feature_process(
        |            user_id string,
        |            order_status int,
        |            active_days_last_30_days long,
        |            total_time_last_30_days double,
        |            days_from_last_active int,
        |            days_since_first_active_in_timewindow int,
        |            active_days_last_14_days long,
        |            total_time_last_14_days double,
        |            active_days_last_7_days long,
        |            total_time_last_7_days double,
        |            active_days_last_3_days long,
        |            total_time_last_3_days double,
        |            total_time_paid_videos_last_30_days double,
        |            total_time_paid_videos_last_14_days double,
        |            total_time_paid_videos_last_7_days double,
        |            total_time_paid_videos_last_3_days double,
        |            total_time_paid_videos_last_1_days double,
        |            total_time_in_package_videos_last_30_days double,
        |            var_time_in_package_videos_last_30_days double,
        |            number_in_package_videos_last_30_days long,
        |            total_time_in_package_videos_last_14_days double,
        |            var_time_in_package_videos_last_14_days double,
        |            number_in_package_videos_last_14_days long,
        |            total_time_in_package_videos_last_7_days double,
        |            var_time_in_package_videos_last_7_days double,
        |            number_in_package_videos_last_7_days long,
        |            total_time_in_package_videos_last_3_days double,
        |            var_time_in_package_videos_last_3_days double,
        |            number_in_package_videos_last_3_days long,
        |            total_time_in_package_videos_last_1_days double,
        |            var_time_in_package_videos_last_1_days double,
        |            number_in_package_videos_last_1_days long,
        |            total_time_children_videos_last_30_days double,
        |            number_children_videos_last_30_days long,
        |            total_time_children_videos_last_14_days double,
        |            number_children_videos_last_14_days long,
        |            total_time_children_videos_last_7_days double,
        |            number_children_videos_last_7_days long,
        |            total_time_children_videos_last_3_days double,
        |            number_children_videos_last_3_days long,
        |            total_time_children_videos_last_1_days double,
        |            number_children_videos_last_1_days long,
        |            total_time_movies_last_30_days double,
        |            total_time_movies_last_14_days double,
        |            total_time_movies_last_7_days double,
        |            total_time_movies_last_3_days double,
        |            total_time_movies_last_1_days double,
        |            total_time_paid_movies_last_30_days double,
        |            total_time_paid_movies_last_14_days double,
        |            total_time_paid_movies_last_7_days double,
        |            total_time_paid_movies_last_3_days double,
        |            total_time_paid_movies_last_1_days double,
        |            active_workdays_last_30_days long,
        |            avg_workdaily_time_videos_last_30_days double,
        |            active_restdays_last_30_days long,
        |            avg_restdaily_time_videos_last_30_days double,
        |            avg_workdaily_time_paid_videos_last_30_days double,
        |            avg_restdaily_time_paid_videos_last_30_days double,
        |            number_packages_purchased long,
        |            total_money_packages_purchased double,
        |            max_money_package_purchased double,
        |            min_money_package_purchased double,
        |            avg_money_package_purchased double,
        |            var_money_package_purchased double,
        |            number_singles_purchased long,
        |            total_money_singles_purchased double,
        |            total_money_consumption double,
        |            number_packages_unpurchased long,
        |            money_packages_unpurchased double,
        |            number_singles_unpurchased long,
        |            money_singles_unpurchased double,
        |            days_since_last_purchase_package int,
        |            days_since_last_click_package int,
        |            number_orders_last_30_days long,
        |            number_paid_orders_last_30_days long,
        |            number_paid_package_last_30_days long,
        |            number_paid_single_last_30_days long,
        |            days_remaining_package int,
        |            video_one_level_preference_1 int,
        |            video_one_level_preference_2 int,
        |            video_one_level_preference_3 int,
        |            video_two_level_preference_1 int,
        |            video_two_level_preference_2 int,
        |            video_two_level_preference_3 int,
        |            movie_two_level_preference_1 int,
        |            movie_two_level_preference_2 int,
        |            movie_two_level_preference_3 int,
        |            single_two_level_preference_1 int,
        |            single_two_level_preference_2 int,
        |            single_two_level_preference_3 int,
        |            in_package_video_two_level_preference_1 int,
        |            in_package_video_two_level_preference_2 int,
        |            in_package_video_two_level_preference_3 int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string, new_or_old string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_feature_process
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category', new_or_old='$new_or_old')
         |SELECT
         |    user_id,
         |    order_status,
         |    active_days_last_30_days,
         |    total_time_last_30_days,
         |    days_from_last_active,
         |    days_since_first_active_in_timewindow,
         |    active_days_last_14_days,
         |    total_time_last_14_days,
         |    active_days_last_7_days,
         |    total_time_last_7_days,
         |    active_days_last_3_days,
         |    total_time_last_3_days,
         |    total_time_paid_videos_last_30_days,
         |    total_time_paid_videos_last_14_days,
         |    total_time_paid_videos_last_7_days,
         |    total_time_paid_videos_last_3_days,
         |    total_time_paid_videos_last_1_days,
         |    total_time_in_package_videos_last_30_days,
         |    var_time_in_package_videos_last_30_days,
         |    number_in_package_videos_last_30_days,
         |    total_time_in_package_videos_last_14_days,
         |    var_time_in_package_videos_last_14_days,
         |    number_in_package_videos_last_14_days,
         |    total_time_in_package_videos_last_7_days,
         |    var_time_in_package_videos_last_7_days,
         |    number_in_package_videos_last_7_days,
         |    total_time_in_package_videos_last_3_days,
         |    var_time_in_package_videos_last_3_days,
         |    number_in_package_videos_last_3_days,
         |    total_time_in_package_videos_last_1_days,
         |    var_time_in_package_videos_last_1_days,
         |    number_in_package_videos_last_1_days,
         |    total_time_children_videos_last_30_days,
         |    number_children_videos_last_30_days,
         |    total_time_children_videos_last_14_days,
         |    number_children_videos_last_14_days,
         |    total_time_children_videos_last_7_days,
         |    number_children_videos_last_7_days,
         |    total_time_children_videos_last_3_days,
         |    number_children_videos_last_3_days,
         |    total_time_children_videos_last_1_days,
         |    number_children_videos_last_1_days,
         |    total_time_movies_last_30_days,
         |    total_time_movies_last_14_days,
         |    total_time_movies_last_7_days,
         |    total_time_movies_last_3_days,
         |    total_time_movies_last_1_days,
         |    total_time_paid_movies_last_30_days,
         |    total_time_paid_movies_last_14_days,
         |    total_time_paid_movies_last_7_days,
         |    total_time_paid_movies_last_3_days,
         |    total_time_paid_movies_last_1_days,
         |    active_workdays_last_30_days,
         |    avg_workdaily_time_videos_last_30_days,
         |    active_restdays_last_30_days,
         |    avg_restdaily_time_videos_last_30_days,
         |    avg_workdaily_time_paid_videos_last_30_days,
         |    avg_restdaily_time_paid_videos_last_30_days,
         |    number_packages_purchased,
         |    total_money_packages_purchased,
         |    max_money_package_purchased,
         |    min_money_package_purchased,
         |    avg_money_package_purchased,
         |    var_money_package_purchased,
         |    number_singles_purchased,
         |    total_money_singles_purchased,
         |    total_money_consumption,
         |    number_packages_unpurchased,
         |    money_packages_unpurchased,
         |    number_singles_unpurchased,
         |    money_singles_unpurchased,
         |    days_since_last_purchase_package,
         |    days_since_last_click_package,
         |    number_orders_last_30_days,
         |    number_paid_orders_last_30_days,
         |    number_paid_package_last_30_days,
         |    number_paid_single_last_30_days,
         |    days_remaining_package,
         |    video_one_level_preference_1,
         |    video_one_level_preference_2,
         |    video_one_level_preference_3,
         |    video_two_level_preference_1,
         |    video_two_level_preference_2,
         |    video_two_level_preference_3,
         |    movie_two_level_preference_1,
         |    movie_two_level_preference_2,
         |    movie_two_level_preference_3,
         |    single_two_level_preference_1,
         |    single_two_level_preference_2,
         |    single_two_level_preference_3,
         |    in_package_video_two_level_preference_1,
         |    in_package_video_two_level_preference_2,
         |    in_package_video_two_level_preference_3
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
