package predict.userpay

import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetErrorMoneySign}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserSplitForPredict {

  val timeLength = 14
  val predictResourceId = Array(100201, 100202) //要预测的套餐id

  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplitForPredict")
      //.master("local[6]")
      //      .enableHiveSupport()
      .getOrCreate()

    val predictTime = args(0) + " " + args(1)

    /**
     * Data Path
     */
    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val predictUsersSavePath = hdfsPath + "data/predict/userpay/predictUsers" + args(0)
    //所有用户id的dataframe
    val allUserPath = hdfsPath + "data/train/userpay/allUsers/user_id.txt"

    /**
     * Get Data
     */
    val df_all_users = spark.read.format("csv").load(allUserPath).toDF(Dic.colUserId)
    printDf("输入 全部用户: ", df_all_users)

    val df_orders = getData(spark, ordersProcessedPath)
    printDf("输入 df_orders", df_orders)

    val df_predict_users = getPredictSetUsers(df_all_users, df_orders, predictTime, timeLength)
    printDf("输出 df_predict_users", df_predict_users)

    //    saveProcessedData(df_predict_users, predictUsersSavePath)


  }


  def getPredictSetUsers(df_all_users: DataFrame, df_orders: DataFrame, predictTime: String, timeLength: Int): DataFrame = {

    val df_order = df_orders.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

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
