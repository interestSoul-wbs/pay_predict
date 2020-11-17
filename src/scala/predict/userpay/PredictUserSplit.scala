package predict.userpay

import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.Dic
import mam.Utils._
import mam.GetSaveData._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import train.userpay.UserSplit.saveUserSamples
import org.apache.spark.sql.functions._

object PredictUserSplit {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var timeWindow: Int = 30

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val predict_time = "2020-09-15 00:00:00"

    // 1 - processed play data
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    // 2 - 所有用户id的dataframe
    val df_all_users = df_plays.select(col(Dic.colUserId)).distinct()

    // 3 - processed order data
    val df_orders = getProcessedOrder(spark, partitiondate, license)

    printDf("df_orders", df_orders)

    // 选择套餐订单
    val df_order_package = df_orders
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4))

    val predictTimePre = calDate(predict_time, days = -timeWindow)

    var predictOrderOld = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && ((col(Dic.colCreationTime).>(predictTimePre) && col(Dic.colCreationTime).<(predict_time))
          || (col(Dic.colOrderEndTime).>(predict_time) && col(Dic.colCreationTime).<(predict_time))))

    val joinKeysUserId = Seq(Dic.colUserId)

    // user_id and order_status - 增加label
    val df_processed_orders = getProcessedOrder(spark, partitiondate, license)

    val df_effective_order = getPredictUsersLabel(df_processed_orders, predict_time)

    //
    predictOrderOld = df_all_users.join(predictOrderOld, joinKeysUserId, "inner")

    val df_predict_old = predictOrderOld
      .select(col(Dic.colUserId))
      .distinct()
      .join(df_effective_order, Seq(Dic.colUserId), "left")
      .na.fill(0)

    printDf("df_predict_old", df_predict_old)

    saveUserSamples(spark, df_predict_old, partitiondate, license, "valid", "old")

    val df_predict_new = df_all_users.except(df_predict_old.select(col(Dic.colUserId)))
      .join(df_effective_order, Seq(Dic.colUserId), "left")
      .na.fill(0)

    printDf("df_predict_new", df_predict_new)

    saveUserSamples(spark, df_predict_new, partitiondate, license, "valid", "new")

    println("需要预测的老用户的数量：" + df_predict_old.count())

    println("需要预测的新用户的数量：" + df_predict_new.count())
  }

  def getPredictUsersLabel(df_processed_order: DataFrame, predict_time: String) = {

    val end_date = getDaysAfter(predict_time, 14)

    val df_result = df_processed_order
      .filter(col(Dic.colCreationTime).gt(lit(predict_time)) && col(Dic.colCreationTime).lt(lit(end_date)))
      .withColumn(Dic.colRank, row_number().over(Window.partitionBy(col(Dic.colUserId)).orderBy(col(Dic.colOrderStatus).desc)))
      .filter(col(Dic.colOrderStatus).>(lit(1)) && col(Dic.colRank).===(lit(1)))
      .select(
        col(Dic.colUserId),
        lit(1).as(Dic.colOrderStatus))

    df_result
  }

  def getDaysAfter(date_now: String, n: Int) = {

    val date_now_formatted = DateTime.parse(date_now, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val date_after = (date_now_formatted + n.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    date_after
  }
}
