package train.singlepoint

import mam.Dic
import mam.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer
import mam.GetSaveData._

object RankTrainDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true") //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "train")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "train")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "train")

    val df_video_profile = getVideoProfile(spark, partitiondate, license, "train")

    val df_video_vector = getVideoVector(spark, partitiondate, license)

    val joinKeysUserId = Seq(Dic.colUserId)

    val joinKeysVideoId = Seq(Dic.colVideoId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)

    val predictWindowStart = "2020-09-01 00:00:00"

    val predictWindowEnd = "2020-09-07 00:00:00"

    //在order订单中选出正样本
    val df_order_single_point = df_orders
      .filter(
        col(Dic.colResourceType).===(0)
          && col(Dic.colCreationTime).>=(predictWindowStart)
          && col(Dic.colCreationTime).<=(predictWindowEnd)
          && col(Dic.colOrderStatus).>(1))
      .select(col(Dic.colUserId), col(Dic.colResourceId), col(Dic.colOrderStatus))
      .withColumnRenamed(Dic.colResourceId, Dic.colVideoId)

    printDf("df_order_single_point", df_order_single_point)

    val df_all_profile_tmp_1 = df_order_single_point
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    printDf("df_all_profile_tmp_1", df_all_profile_tmp_1)

    println("第一部分数据条数：" + df_all_profile_tmp_1.count())

    //第一部分的负样本
    val df_userid_videoid = df_all_profile_tmp_1.select(col(Dic.colUserId), col(Dic.colVideoId))

    printDf("df_userid_videoid", df_userid_videoid)

    //设置负样本中选择多少个video作为负样本中的video
    val df_popular_video = df_userid_videoid
      .groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))

    printDf("df_popular_video", df_popular_video)

    val df_all_profile_tmp_2 = df_userid_videoid
      .select(col(Dic.colUserId))
      .distinct()
      .crossJoin(df_popular_video)
      .except(df_userid_videoid)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第二部分数据条数：" + df_all_profile_tmp_2.count())

    printDf("df_all_profile_tmp_2", df_all_profile_tmp_2)

    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val negativeUserN = 10 * df_userid_videoid.select(col(Dic.colUserId)).distinct().count()
    val df_neg_users = df_user_profile.select(col(Dic.colUserId)).except(df_userid_videoid.select(col(Dic.colUserId))).limit(negativeUserN.toInt)
    var df_all_profile_tmp_3 = df_neg_users
      .crossJoin(df_popular_video)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
      .join(df_user_profile, joinKeysUserId, "inner")
      .join(df_video_profile, joinKeysVideoId, "inner")

    println("第三部分数据条数：" + df_all_profile_tmp_3.count())

    printDf("df_all_profile_tmp_3", df_all_profile_tmp_3)

    val df_result_tmp_1 = df_all_profile_tmp_1
      .union(df_all_profile_tmp_2)
      .union(df_all_profile_tmp_3)
      .join(df_video_vector, joinKeysVideoId, "left")

    printDf("df_result_tmp_1", df_result_tmp_1)

    val seqColList = getNumerableColsSeq(df_result_tmp_1)

    val df_result = df_result_tmp_1
      .select(seqColList.map(df_result_tmp_1.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow, Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
      .na.fill(0)

    println("总样本的条数" + df_result.count())

    printDf("df_result", df_result)

    saveSinglepointRankData(spark, df_result, partitiondate, license, "train")
  }

  def saveSinglepointRankData(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_singlepoint_rank(
        |        user_id string,
        |        video_id string,
        |        order_status double,
        |        active_days_last_30_days long,
        |        total_time_last_30_days double,
        |        days_from_last_active int,
        |        days_since_first_active_in_timewindow int,
        |        active_days_last_14_days long,
        |        total_time_last_14_days double,
        |        active_days_last_7_days long,
        |        total_time_last_7_days double,
        |        active_days_last_3_days long,
        |        total_time_last_3_days double,
        |        total_time_paid_videos_last_30_days double,
        |        total_time_paid_videos_last_14_days double,
        |        total_time_paid_videos_last_7_days double,
        |        total_time_paid_videos_last_3_days double,
        |        total_time_paid_videos_last_1_days double,
        |        total_time_in_package_videos_last_30_days double,
        |        var_time_in_package_videos_last_30_days double,
        |        number_in_package_videos_last_30_days long,
        |        total_time_in_package_videos_last_14_days double,
        |        var_time_in_package_videos_last_14_days double,
        |        number_in_package_videos_last_14_days long,
        |        total_time_in_package_videos_last_7_days double,
        |        var_time_in_package_videos_last_7_days double,
        |        number_in_package_videos_last_7_days long,
        |        total_time_in_package_videos_last_3_days double,
        |        var_time_in_package_videos_last_3_days double,
        |        number_in_package_videos_last_3_days long,
        |        total_time_in_package_videos_last_1_days double,
        |        var_time_in_package_videos_last_1_days double,
        |        number_in_package_videos_last_1_days long,
        |        total_time_children_videos_last_30_days double,
        |        number_children_videos_last_30_days long,
        |        total_time_children_videos_last_14_days double,
        |        number_children_videos_last_14_days long,
        |        total_time_children_videos_last_7_days double,
        |        number_children_videos_last_7_days long,
        |        total_time_children_videos_last_3_days double,
        |        number_children_videos_last_3_days long,
        |        total_time_children_videos_last_1_days double,
        |        number_children_videos_last_1_days long,
        |        total_time_movies_last_30_days double,
        |        total_time_movies_last_14_days double,
        |        total_time_movies_last_7_days double,
        |        total_time_movies_last_3_days double,
        |        total_time_movies_last_1_days double,
        |        total_time_paid_movies_last_30_days double,
        |        total_time_paid_movies_last_14_days double,
        |        total_time_paid_movies_last_7_days double,
        |        total_time_paid_movies_last_3_days double,
        |        total_time_paid_movies_last_1_days double,
        |        active_workdays_last_30_days long,
        |        avg_workdaily_time_videos_last_30_days double,
        |        active_restdays_last_30_days long,
        |        avg_restdaily_time_videos_last_30_days double,
        |        avg_workdaily_time_paid_videos_last_30_days double,
        |        avg_restdaily_time_paid_videos_last_30_days double,
        |        number_packages_purchased long,
        |        total_money_packages_purchased double,
        |        max_money_package_purchased double,
        |        min_money_package_purchased double,
        |        avg_money_package_purchased double,
        |        var_money_package_purchased double,
        |        number_singles_purchased long,
        |        total_money_singles_purchased double,
        |        total_money_consumption double,
        |        number_packages_unpurchased long,
        |        money_packages_unpurchased double,
        |        number_singles_unpurchased long,
        |        money_singles_unpurchased double,
        |        days_since_last_purchase_package int,
        |        days_since_last_click_package int,
        |        number_orders_last_30_days long,
        |        number_paid_orders_last_30_days long,
        |        number_paid_package_last_30_days long,
        |        number_paid_single_last_30_days long,
        |        days_remaining_package int,
        |        video_time double,
        |        score double,
        |        number_of_plays_in_30_days long,
        |        number_of_views_within_30_days long,
        |        number_of_plays_in_14_days long,
        |        number_of_views_within_14_days long,
        |        number_of_plays_in_7_days long,
        |        number_of_views_within_7_days long,
        |        number_of_plays_in_3_days long,
        |        number_of_views_within_3_days long,
        |        abs_of_number_of_days_between_storage_and_current int,
        |        number_of_times_purchased_within_30_days long,
        |        number_of_times_purchased_within_14_days long,
        |        number_of_times_purchased_within_7_days long,
        |        number_of_times_purchased_within_3_days long,
        |        number_of_times_purchased_total long,
        |        v_0 double,
        |        v_1 double,
        |        v_2 double,
        |        v_3 double,
        |        v_4 double,
        |        v_5 double,
        |        v_6 double,
        |        v_7 double,
        |        v_8 double,
        |        v_9 double,
        |        v_10 double,
        |        v_11 double,
        |        v_12 double,
        |        v_13 double,
        |        v_14 double,
        |        v_15 double,
        |        v_16 double,
        |        v_17 double,
        |        v_18 double,
        |        v_19 double,
        |        v_20 double,
        |        v_21 double,
        |        v_22 double,
        |        v_23 double,
        |        v_24 double,
        |        v_25 double,
        |        v_26 double,
        |        v_27 double,
        |        v_28 double,
        |        v_29 double,
        |        v_30 double,
        |        v_31 double,
        |        v_32 double,
        |        v_33 double,
        |        v_34 double,
        |        v_35 double,
        |        v_36 double,
        |        v_37 double,
        |        v_38 double,
        |        v_39 double,
        |        v_40 double,
        |        v_41 double,
        |        v_42 double,
        |        v_43 double,
        |        v_44 double,
        |        v_45 double,
        |        v_46 double,
        |        v_47 double,
        |        v_48 double,
        |        v_49 double,
        |        v_50 double,
        |        v_51 double,
        |        v_52 double,
        |        v_53 double,
        |        v_54 double,
        |        v_55 double,
        |        v_56 double,
        |        v_57 double,
        |        v_58 double,
        |        v_59 double,
        |        v_60 double,
        |        v_61 double,
        |        v_62 double,
        |        v_63 double)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_singlepoint_rank
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |        user_id,
         |        video_id,
         |        order_status,
         |        active_days_last_30_days,
         |        total_time_last_30_days,
         |        days_from_last_active,
         |        days_since_first_active_in_timewindow,
         |        active_days_last_14_days,
         |        total_time_last_14_days,
         |        active_days_last_7_days,
         |        total_time_last_7_days,
         |        active_days_last_3_days,
         |        total_time_last_3_days,
         |        total_time_paid_videos_last_30_days,
         |        total_time_paid_videos_last_14_days,
         |        total_time_paid_videos_last_7_days,
         |        total_time_paid_videos_last_3_days,
         |        total_time_paid_videos_last_1_days,
         |        total_time_in_package_videos_last_30_days,
         |        var_time_in_package_videos_last_30_days,
         |        number_in_package_videos_last_30_days,
         |        total_time_in_package_videos_last_14_days,
         |        var_time_in_package_videos_last_14_days,
         |        number_in_package_videos_last_14_days,
         |        total_time_in_package_videos_last_7_days,
         |        var_time_in_package_videos_last_7_days,
         |        number_in_package_videos_last_7_days,
         |        total_time_in_package_videos_last_3_days,
         |        var_time_in_package_videos_last_3_days,
         |        number_in_package_videos_last_3_days,
         |        total_time_in_package_videos_last_1_days,
         |        var_time_in_package_videos_last_1_days,
         |        number_in_package_videos_last_1_days,
         |        total_time_children_videos_last_30_days,
         |        number_children_videos_last_30_days,
         |        total_time_children_videos_last_14_days,
         |        number_children_videos_last_14_days,
         |        total_time_children_videos_last_7_days,
         |        number_children_videos_last_7_days,
         |        total_time_children_videos_last_3_days,
         |        number_children_videos_last_3_days,
         |        total_time_children_videos_last_1_days,
         |        number_children_videos_last_1_days,
         |        total_time_movies_last_30_days,
         |        total_time_movies_last_14_days,
         |        total_time_movies_last_7_days,
         |        total_time_movies_last_3_days,
         |        total_time_movies_last_1_days,
         |        total_time_paid_movies_last_30_days,
         |        total_time_paid_movies_last_14_days,
         |        total_time_paid_movies_last_7_days,
         |        total_time_paid_movies_last_3_days,
         |        total_time_paid_movies_last_1_days,
         |        active_workdays_last_30_days,
         |        avg_workdaily_time_videos_last_30_days,
         |        active_restdays_last_30_days,
         |        avg_restdaily_time_videos_last_30_days,
         |        avg_workdaily_time_paid_videos_last_30_days,
         |        avg_restdaily_time_paid_videos_last_30_days,
         |        number_packages_purchased,
         |        total_money_packages_purchased,
         |        max_money_package_purchased,
         |        min_money_package_purchased,
         |        avg_money_package_purchased,
         |        var_money_package_purchased,
         |        number_singles_purchased,
         |        total_money_singles_purchased,
         |        total_money_consumption,
         |        number_packages_unpurchased,
         |        money_packages_unpurchased,
         |        number_singles_unpurchased,
         |        money_singles_unpurchased,
         |        days_since_last_purchase_package,
         |        days_since_last_click_package,
         |        number_orders_last_30_days,
         |        number_paid_orders_last_30_days,
         |        number_paid_package_last_30_days,
         |        number_paid_single_last_30_days,
         |        days_remaining_package,
         |        video_time,
         |        score,
         |        number_of_plays_in_30_days,
         |        number_of_views_within_30_days,
         |        number_of_plays_in_14_days,
         |        number_of_views_within_14_days,
         |        number_of_plays_in_7_days,
         |        number_of_views_within_7_days,
         |        number_of_plays_in_3_days,
         |        number_of_views_within_3_days,
         |        abs_of_number_of_days_between_storage_and_current,
         |        number_of_times_purchased_within_30_days,
         |        number_of_times_purchased_within_14_days,
         |        number_of_times_purchased_within_7_days,
         |        number_of_times_purchased_within_3_days,
         |        number_of_times_purchased_total,
         |        v_0,
         |        v_1,
         |        v_2,
         |        v_3,
         |        v_4,
         |        v_5,
         |        v_6,
         |        v_7,
         |        v_8,
         |        v_9,
         |        v_10,
         |        v_11,
         |        v_12,
         |        v_13,
         |        v_14,
         |        v_15,
         |        v_16,
         |        v_17,
         |        v_18,
         |        v_19,
         |        v_20,
         |        v_21,
         |        v_22,
         |        v_23,
         |        v_24,
         |        v_25,
         |        v_26,
         |        v_27,
         |        v_28,
         |        v_29,
         |        v_30,
         |        v_31,
         |        v_32,
         |        v_33,
         |        v_34,
         |        v_35,
         |        v_36,
         |        v_37,
         |        v_38,
         |        v_39,
         |        v_40,
         |        v_41,
         |        v_42,
         |        v_43,
         |        v_44,
         |        v_45,
         |        v_46,
         |        v_47,
         |        v_48,
         |        v_49,
         |        v_50,
         |        v_51,
         |        v_52,
         |        v_53,
         |        v_54,
         |        v_55,
         |        v_56,
         |        v_57,
         |        v_58,
         |        v_59,
         |        v_60,
         |        v_61,
         |        v_62,
         |        v_63
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
