package train.userpay


import org.apache.spark.sql.functions._
import mam.Dic
import mam.GetSaveData.saveProcessedData
import mam.Utils.{calDate, getData, printDf, sysParamSetting, udfGetErrorMoneySign}
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserSplitForTrain {

  val timeLength = 14
  val predictResourceId = Array(100201, 100202) //要预测的套餐id


  def main(args: Array[String]): Unit = {
    sysParamSetting()

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplitForTrain")
      //.master("local[6]")
      //.enableHiveSupport()
      .getOrCreate()

    val trainTime = args(0) + " " + args(1)
    println("trainTime", trainTime)


    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath = ""
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val trainSetUsersPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)
    val allUserPath = hdfsPath + "data/train/userpay/allUsers/user_id.txt"

    val df_orders = getData(spark, ordersProcessedPath)
    printDf("df_orders", df_orders)

    //所有用户id的dataframe  Hisense data
    val df_all_Users = spark.read.format("csv").load(allUserPath).toDF(Dic.colUserId)
    printDf("全部用户: ", df_all_Users)


    /**
     * 训练集正负样本选取
     */
    val df_all_train_users = getTrainSetUsers(df_all_Users, df_orders, trainTime, timeLength, predictResourceId)
    printDf("df_all_train_users", df_all_train_users)
    saveProcessedData(df_all_train_users, trainSetUsersPath)

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
    val df_order = df_orders.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

    //金额异常用户
    val df_illegal_users = df_order.filter(
      col(Dic.colCreationTime) >= trainTime and col(Dic.colCreationTime) < calDate(trainTime, timeLength)
        && col(Dic.colResourceType).>(0) and col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && (col(Dic.colIsMoneyError) === 1)
    ).select(Dic.colUserId).distinct()

    printDf("df_illegal_users", df_illegal_users)


    //  正样本
    val df_train_pos = df_order.filter(
      col(Dic.colCreationTime) >= trainTime and col(Dic.colCreationTime) < calDate(trainTime, timeLength)
        && col(Dic.colResourceType).>(0) and col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && col(Dic.colOrderStatus).>(1)
    ).select(Dic.colUserId).distinct()

    //去掉金额异常用户
    val df_train_pos_users = df_train_pos.except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(1))

    printDf("trainPosUsers", df_train_pos_users)

    //负样本
    val df_all_neg_users = df_allUsers.except(df_train_pos_users.select(Dic.colUserId))
      .except(df_illegal_users) //except 保证column一致

    val df_train_neg_users = df_all_neg_users.sample(1).limit(10 * df_train_pos_users.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))
    printDf("df_trainNegUsers", df_train_neg_users)

    df_train_pos_users.union(df_train_neg_users).distinct().sample(1)

  }


}
