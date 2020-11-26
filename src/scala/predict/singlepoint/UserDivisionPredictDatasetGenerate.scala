package predict.singlepoint

import com.github.nscala_time.time.Imports._
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UserDivisionPredictDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var negativeN: Int = 10
  var date: DateTime = _
  var fifteenDaysAgo: String = _
  var oneDayAgo: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    fifteenDaysAgo = (date - 15.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))
    oneDayAgo = (date - 1.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    println("fifteenDaysAgo is : " + fifteenDaysAgo) // 例： 2020-09-16 00:00:00

    println("oneDayAgo is : " + oneDayAgo) // 例： 2020-09-30 00:00:00

    val spark = SparkSession.builder().enableHiveSupport().config("spark.sql.crossJoin.enabled", "true").getOrCreate()

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "valid")

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)
    //将全部用户作为预测的样本，这时候标签是未知的，所以不用构造样本
 /**
    val predictWindowStart = fifteenDaysAgo

    val predictWindowEnd = oneDayAgo

    printDf("df_orders", df_orders)

    //在预测时间窗口内的单点视频的订单
    val df_single_paid_orders = df_orders
      .filter(
        col(Dic.colCreationTime).gt(lit(predictWindowStart))
          && col(Dic.colCreationTime).lt(lit(predictWindowEnd))
          && col(Dic.colResourceType).===(0)
          && col(Dic.colOrderStatus).>(1))

    printDf("df_single_paid_orders", df_single_paid_orders)

    //过滤掉偏好
    val seqColList = getFilteredColList(df_user_profile)

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
    **/
    
    //过滤掉偏好
    val seqColList = getFilteredColList(df_user_profile)
    val df_all_users=df_user_profile.select(seqColList.map(df_user_profile.col(_)): _*)
    printDf("df_all_users", df_all_users)
    
    
    val df_all_users_not_null = df_all_users
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()
   
    printDf("df_all_users_not_null", df_all_users_not_null)

    saveSinglepointUserDivisionData(spark, df_all_users_not_null, partitiondate, license, "valid")
  }
}
