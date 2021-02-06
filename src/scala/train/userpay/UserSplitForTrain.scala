package train.userpay


import org.apache.spark.sql.functions._
import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getAllUsers, getAllUsersPlayAndOrder, getProcessedOrder, saveProcessedData, saveTrainUsers}
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetErrorMoneySign}
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserSplitForTrain {

  val timeLength = 14
  val predictResourceId = Array(100201,100202,101101,101103,101301,101601,101701,101801,101803,101810) //要预测的套餐id


  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    val trainTime = args(0) + " " + args(1)
    println("trainTime", trainTime)

    // 2 Get Data
    val df_orders = getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)

    //所有用户id的dataframe  Hisense data
//    val df_all_users = getAllUsers(spark)
    val df_all_users = getAllUsersPlayAndOrder(spark)
    printDf("输入 df_all_Users", df_all_users)

    // 3 训练集正负样本选取
    val df_all_train_users = getTrainSetUsers(df_all_users, df_orders, trainTime, timeLength, predictResourceId)
    printDf("输出 df_all_train_users", df_all_train_users)

    // 4 Save Train Users
    saveTrainUsers(trainTime, df_all_train_users)
    println("Train Users Save Done!")
  }

  def getTrainSetUsers(df_allUsers: DataFrame, df_orders: DataFrame, trainTime: String, timeLength: Int, predictResourceId: Array[Int]): DataFrame = {

    /**
     * 训练集正负样本选取
     * 正样本：选择在order中trainTime到 未来14天 内的成功购买用户
     * 负样本：选择正样本以外的10倍用户
     */

    /**
     * 标记金额信息异常用户 money is 0 or 1 RMB
     */
    val df_order = df_orders
      .withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

    //金额异常用户
    val df_illegal_users = df_order
      .filter(
        col(Dic.colCreationTime) >= trainTime
          && col(Dic.colCreationTime) < calDate(trainTime, timeLength)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4)
          && (col(Dic.colResourceId).isin(predictResourceId: _*))
          && (col(Dic.colIsMoneyError) === 1)
      ).select(Dic.colUserId).distinct()

    //  正样本
    val df_train_pos_users = df_order
      .filter(
        col(Dic.colCreationTime) >= trainTime
          && col(Dic.colCreationTime) < calDate(trainTime, timeLength)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4)
          && (col(Dic.colResourceId).isin(predictResourceId: _*))
          && col(Dic.colOrderStatus).>(1)
      ).select(Dic.colUserId).distinct()
      .except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(1))

    //负样本
    val df_all_neg_users = df_allUsers
      .except(df_train_pos_users.select(Dic.colUserId))
      .except(df_illegal_users) //except 保证column一致

    val df_train_neg_users = df_all_neg_users
      .sample(1).limit(10 * df_train_pos_users.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))

    df_train_pos_users.union(df_train_neg_users).distinct().sample(1)

  }


}
