package predict.userpay

import mam.GetSaveData.{getAllUsers, getAllUsersPlayAndOrder, getProcessedOrder, savePredictUsers, saveProcessedData, saveTrainUsers}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetErrorMoneySign}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import mam.{Dic, SparkSessionInit}

object UserSplitForPredict {

  val timeLength = 14
  val predictResourceId = Array(100201,100202,101101,101103,101301,101601,101701,101801,101803,101810) //要预测的套餐id


  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    val predictTime = args(0) + " " + args(1)
    println("predictTime", predictTime)

    // 2 Get Data
    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    //所有用户id的dataframe  Hisense data
    val df_all_users = getAllUsers(spark)
//    val df_all_users = getAllUsersPlayAndOrder(spark)
    printDf("输入 df_all_Users", df_all_users)

    // 3
    val df_predict_users = getPredictSetUsers(df_all_users, df_orders, predictTime, timeLength)
    printDf("输出 df_predict_users", df_predict_users)

    // 4 Save Train Users

    savePredictUsers(predictTime, df_predict_users)
    println("Save Predict Users Done!")


  }


  def getPredictSetUsers(df_all_users: DataFrame, df_orders: DataFrame, predictTime: String, timeLength: Int): DataFrame = {

    val df_order = df_orders
      .withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

    //金额异常用户
    val df_illegal_users = df_order.filter(
      col(Dic.colCreationTime) >= predictTime
        && col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && col(Dic.colResourceType).>(0)
        && col(Dic.colResourceType).<(4)
        && col(Dic.colResourceId).isin(predictResourceId: _*)
        && (col(Dic.colIsMoneyError) === 1)
    ).select(Dic.colUserId).distinct()

    printDf("df_illegal_users", df_illegal_users)


    //  正样本
    val df_predict_pos_users = df_order.filter(
      col(Dic.colCreationTime) >= predictTime
        && col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && col(Dic.colResourceType).>(0)
        && col(Dic.colResourceType).<(4)
        && col(Dic.colResourceId).isin(predictResourceId: _*)
        && col(Dic.colOrderStatus).>(1)
    ).select(Dic.colUserId).distinct()
      .except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(1))

    printDf("df_predict_pos_users", df_predict_pos_users)

    //全部负样本
    val df_predict_neg_users = df_all_users
      .except(df_predict_pos_users.select(Dic.colUserId))
      .except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(0))

    printDf("df_predict_neg_users", df_predict_neg_users)

    df_predict_pos_users.union(df_predict_neg_users).sample(1)

  }


}
