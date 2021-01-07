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

    val df_user_profile_play_part = getUserProfilePlayPart(partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(partitiondate, license, "valid")

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)
    //将全部用户作为预测的样本，这时候标签是未知的，所以不用构造样本

    //过滤掉偏好
    val seqColList = getFilteredColList(df_user_profile)
    val df_all_users = df_user_profile.select(seqColList.map(df_user_profile.col(_)): _*)
    printDf("df_all_users", df_all_users)

    val df_all_users_not_null = df_all_users
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()
      .withColumn(Dic.colOrderStatus, lit(-1))

    printDf("df_all_users_not_null", df_all_users_not_null)

    println("total users: ", df_all_users_not_null.count())

    // MinMaxScaler
    val exclude_cols = Array(Dic.colUserId, Dic.colOrderStatus)

    val df_result = scaleData(df_all_users_not_null, exclude_cols)

    printDf("df_result", df_result)

    saveSinglepointUserDivisionData(df_result, partitiondate, license, "predict")
  }
}
