package predict.userpay

import mam.Dic
import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.functions.{col, udf, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ArrayBuffer

object GetLableofPredictData {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var date: DateTime = _
  var sixteenDaysAgo: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    sixteenDaysAgo = (date - 16.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 1 - 抽取有效的订单信息
    val df_processed_orders = getProcessedOrder(partitiondate, license)

    val df_effective_order = getPredictUsersLabel(df_processed_orders, sixteenDaysAgo)

    // 2 - predict中 Old 用户打上label
    val df_old_user_list_raw = getTrainUser(partitiondate, license, "valid", "old")

    joinLabelAndSave(df_old_user_list_raw, df_effective_order,partitiondate, license, "valid_true", "old")

    // 3 - predict中 New 用户打上label
    val df_new_user_list_raw = getTrainUser(partitiondate, license, "valid", "new")

    joinLabelAndSave(df_new_user_list_raw, df_effective_order,partitiondate, license, "valid_true", "new")
  }

  def joinLabelAndSave(df_user_list_raw: DataFrame, df_effective_order: DataFrame,
                       partitiondate: String, license: String, category: String, new_or_old: String) = {

    val df_user_label = df_user_list_raw
      .select(
        col(Dic.colUserId))
      .join(df_effective_order, Seq(Dic.colUserId), "left")
      .na.fill(0)

    saveUserSplitResult(df_user_label, partitiondate, license, category, new_or_old)
  }

}
