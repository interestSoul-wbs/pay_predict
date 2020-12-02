package predict.userpay

import mam.Dic
import mam.Utils.{calDate, getData, printDf, saveProcessedData, udfGetErrorMoneySign}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserSplitForPredict {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplitForPredict")
      .master("local[6]")
      .getOrCreate()

    val predictTime = args(0) + " " + args(1)
    val timeLength = 14

    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val predictUsersPath = hdfsPath + "data/predict/userpay/predictUsers" + args(0)

    //所有用户id的dataframe
    val allUserPath = hdfsPath + "data/train/userpay/allUsers/user_id.txt"
    val df_allUsers = spark.read.format("csv").load(allUserPath).toDF(Dic.colUserId)
    printDf("全部用户: ", df_allUsers)

    val df_orders = getData(spark, ordersProcessedPath)

    val df_allPredictUsers = getPredictSetUsers(df_allUsers, df_orders, predictTime, timeLength)
    saveProcessedData(df_allPredictUsers, predictUsersPath)


  }


  def getPredictSetUsers(df_allUsers: DataFrame, df_orders: DataFrame, predictTime: String, timeLength: Int): DataFrame = {

    val df_order = df_orders.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))
    val predictResourceId = Array(100201, 100202) //要预测的套餐id

    //金额异常用户

    val df_illegalUsers = df_order.filter(
      col(Dic.colCreationTime) >= predictTime and col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && (col(Dic.colResourceType) > 0 and col(Dic.colResourceType) < 4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && (col(Dic.colIsMoneyError) === 1)
    ).select(Dic.colUserId).distinct()


    //  正样本
    var df_predictPosUsers = df_order.filter(
      col(Dic.colCreationTime) >= predictTime and col(Dic.colCreationTime) < calDate(predictTime, timeLength)
        && col(Dic.colResourceType).>(0) and col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && col(Dic.colOrderStatus).>(1)
    ).select(Dic.colUserId).distinct()

    df_predictPosUsers = df_predictPosUsers.except(df_illegalUsers)
      .withColumn(Dic.colOrderStatus, lit(1))

    printDf("predictPosUsers", df_predictPosUsers)

    //负样本
    val df_allNegUsers = df_allUsers.except(df_predictPosUsers.select(Dic.colUserId))
      .except(df_illegalUsers)

    val df_predictNegUsers = df_allNegUsers.sample(1).limit(10 * df_predictPosUsers.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))

    printDf("df_PredictNegUsers", df_predictNegUsers)

    df_predictPosUsers.union(df_predictNegUsers).sample(1)

  }


}
