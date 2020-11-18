package train.singlepoint

import mam.Dic
import mam.Utils._
import org.apache.spark.sql.{SaveMode, SparkSession}
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
  }

}
