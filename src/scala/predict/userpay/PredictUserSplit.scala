package predict.userpay

import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.Dic
import mam.Utils._
import mam.GetSaveData._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.functions._

object PredictUserSplit {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var timeWindow: Int = 30
  var date: DateTime = _
  var nDaysFromStartDate: Int = _
  var dataSplitDate: String = _

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // union1.x
    sector = args(3).toInt
    nDaysFromStartDate = args(4).toInt // train - 0/predict - 14 - 各跑一次

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    dataSplitDate = (date - (30 - nDaysFromStartDate).days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 1 - processed play data
    val df_plays = getProcessedPlay(partitiondate, license, vodVersion, sector)

    // 2 - 所有用户id的dataframe
    val df_all_users = df_plays.select(col(Dic.colUserId)).distinct()

    // 3 - processed order data
    val df_orders = getProcessedOrder(partitiondate, license, vodVersion, sector)

    // 选择套餐订单
    val df_order_package = df_orders
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4))

    val predictTimePre = calDate(dataSplitDate, days = -timeWindow)

    var predictOrderOld = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && ((col(Dic.colCreationTime).>(predictTimePre) && col(Dic.colCreationTime).<(dataSplitDate))
          || (col(Dic.colOrderEndTime).>(dataSplitDate) && col(Dic.colCreationTime).<(dataSplitDate))))

    val joinKeysUserId = Seq(Dic.colUserId)

    // user_id and order_status - 增加label
    // 因为存储时，hive中的order_status必须有值，此处，测试集的标签列用 -1 进行填充；
    predictOrderOld = df_all_users.join(predictOrderOld, joinKeysUserId, "inner")

    val df_predict_old = predictOrderOld
      .select(col(Dic.colUserId))
      .distinct()
      .withColumn(Dic.colOrderStatus, lit(-1))
      .na.fill(0)

    printDf("df_predict_old", df_predict_old)

    saveUserSplitResult(df_predict_old, partitiondate, license, "old", vodVersion, sector, nDaysFromStartDate)

    val df_predict_new = df_all_users.except(df_predict_old.select(col(Dic.colUserId)))
      .withColumn(Dic.colOrderStatus, lit(-1))
      .na.fill(0)

    printDf("df_predict_new", df_predict_new)

    saveUserSplitResult(df_predict_new, partitiondate, license, "new", vodVersion, sector, nDaysFromStartDate)

    println("需要预测的老用户的数量：" + df_predict_old.count())

    println("需要预测的新用户的数量：" + df_predict_new.count())
  }
}
