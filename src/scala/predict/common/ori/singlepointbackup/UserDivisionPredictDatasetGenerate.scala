package predict.common.ori.singlepointbackup

import mam.Dic
import mam.GetSaveData.{getUserProfileOrderPart, getUserProfilePlayPart, getuserProfilePreferencePart, saveSinglepointUserDivisionData}
import mam.Utils.{getFilteredColList, printDf}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object UserDivisionPredictDatasetGenerate {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().config("spark.sql.crossJoin.enabled", "true").getOrCreate()

    val df_user_profile_play_part = getUserProfilePlayPart(spark, partitiondate, license, "valid")

    val df_user_profile_preference_part = getuserProfilePreferencePart(spark, partitiondate, license, "valid")

    val df_user_profile_order_part = getUserProfileOrderPart(spark, partitiondate, license, "valid")

    val joinKeysUserId = Seq(Dic.colUserId)

    val df_user_profile = df_user_profile_play_part
      .join(df_user_profile_preference_part, joinKeysUserId, "left")
      .join(df_user_profile_order_part, joinKeysUserId, "left")

    printDf("df_user_profile", df_user_profile)

    //过滤掉偏好
    val seqColList = getFilteredColList(df_user_profile)

    val df_result = df_user_profile
      .select(seqColList.map(df_user_profile.col(_)): _*)
      .na.fill(30, Seq(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()
      .withColumn(Dic.colOrderStatus, lit(-1))

    printDf("df_result", df_result)

    saveSinglepointUserDivisionData(spark, df_result, partitiondate, license, "valid")
  }

}
