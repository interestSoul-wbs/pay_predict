package train.singlepoint

import mam.Dic
import mam.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import mam.GetSaveData._

object UserDivisionTrainDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().config("spark.sql.crossJoin.enabled", "true").getOrCreate()

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "train")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "train")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "train")

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)

    val predictWindowStart = "2020-09-01 00:00:00"

    val predictWindowEnd = "2020-09-07 00:00:00"

    //在预测时间窗口内的单点视频的订单
    val df_single_paid_orders = df_orders.filter(
      col(Dic.colCreationTime).>=(predictWindowStart)
        && col(Dic.colCreationTime).<=(predictWindowEnd)
        && col(Dic.colResourceType).===(0)
        && col(Dic.colOrderStatus).>(1))

    printDf("df_single_paid_orders", df_single_paid_orders)

    //过滤掉偏好
    val seqColList = getSeqColList(df_user_profile)

    //找出订购了单点视频的用户的用户画像作为正样本
    val df_user_paid_profile = df_user_profile
      .join(df_single_paid_orders, joinKeysUserId, "inner")
      .select(seqColList.map(df_user_profile.col(_)): _*)
      .dropDuplicates(Dic.colUserId)

    printDf("df_single_paid_orders", df_single_paid_orders)

    println("正样本的条数为：" + df_user_paid_profile.count())
    val positiveCount = df_user_paid_profile.count().toInt

    //构造负样本，确定正负样本的比例为1:10
    val df_neg_users = df_user_profile
      .select(seqColList.map(df_user_profile.col(_)): _*)
      .except(df_user_paid_profile)
      .sample(fraction = 1.0)
      .limit(negativeN * positiveCount)
    println("负样本的条数为：" + df_neg_users.count())

    printDf("df_neg_users", df_neg_users)

    //为正负样本分别添加标签
    val df_neg_users_with_label = df_neg_users.withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)
    val df_user_paid_with_label = df_user_paid_profile.withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)))

    //将正负样本组合在一起并shuffle
    val df_all_users = df_user_paid_with_label.union(df_neg_users_with_label).sample(fraction = 1.0)
    println("总样本的条数为：" + df_all_users.count())

    printDf("df_all_users", df_all_users)

    val df_all_users_not_null = df_all_users
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()

    printDf("df_all_users_not_null", df_all_users_not_null)

    saveSinglepointUserDivisionData(spark, df_all_users_not_null, partitiondate, license, "train")
  }


  def saveSinglepointUserDivisionData(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_singlepoint_user_division(
        |            user_id string,
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
        |            order_status int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_singlepoint_user_division
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |        user_id,
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
         |        order_status
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
