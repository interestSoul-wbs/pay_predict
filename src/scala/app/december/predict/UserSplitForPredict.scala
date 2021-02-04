package app.december.predict

import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.joda.time.DateTime
import rs.common.SparkSessionInit

object UserSplitForPredict {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var category: String = _
  var date: DateTime = _
  val timeLength = 14
  val predictResourceId = Array(100201, 100202) //要预测的套餐id

  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    SparkSessionInit.init()

//    val predictTime = partitiondateToStandard(partitiondate)
//
//    println("predictTime", predictTime)
//
//    // 2 Get Data
//    val df_orders = getProcessedOrderV2(partitiondate, license, vodVersion, sector)
//    printDf("输入 df_orders", df_orders)
//
//    //所有用户id的dataframe  Hisense data
    val df_all_users = getAllUsers(partitiondate, license, vodVersion, sector)
    .select(
      col(Dic.colUserId)
      ,lit(-1).as(Dic.colOrderStatus))

    printDf("输入 df_all_Users", df_all_users)
//
//    // 3
//    val df_predict_users = getPredictSetUsers(df_all_users, df_orders, predictTime, timeLength)
//    printDf("输出 df_predict_users", df_predict_users)

    // 4 Save Train Users
    saveUsersSplitDataV2(df_all_users, partitiondate, license, vodVersion, sector, category)
  }


  def getPredictSetUsers(df_all_users: DataFrame, df_orders: DataFrame, predictTime: String, timeLength: Int): DataFrame = {

    val df_order = df_orders
      .withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

    //金额异常用户
    val df_illegal_users = df_order.filter(
      col(Dic.colCreationTime) >= predictTime
        && col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && (col(Dic.colResourceType) > 0
        && col(Dic.colResourceType) < 4)
        && (col(Dic.colResourceId) === predictResourceId(0)
        or col(Dic.colResourceId) === predictResourceId(1))
        && (col(Dic.colIsMoneyError) === 1)
    ).select(Dic.colUserId).distinct()

    //  正样本
    val df_predict_pos_users = df_order.filter(
      col(Dic.colCreationTime) >= predictTime
        && col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && col(Dic.colResourceType).>(0)
        && col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0)
        or col(Dic.colResourceId) === predictResourceId(1))
        && col(Dic.colOrderStatus).>(1)
    ).select(Dic.colUserId).distinct()
      .except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(1))

    //负样本
    val df_all_neg_users = df_all_users
      .except(df_predict_pos_users.select(Dic.colUserId))
      .except(df_illegal_users)

    val df_predict_neg_users = df_all_neg_users
      .sample(1).limit(10 * df_predict_pos_users.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))

    df_predict_pos_users.union(df_predict_neg_users).sample(1)
  }
}
