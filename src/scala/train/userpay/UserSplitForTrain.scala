package train.userpay


import org.apache.spark.sql.functions._
import mam.Dic
import mam.Utils.{calDate, getData, printDf, saveProcessedData, udfGetErrorMoneySign}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserSplitForTrain {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplitForTrain")
      .master("local[6]")
      .getOrCreate()

    val trainTime = args(0) + " " + args(1)
    println(trainTime)


    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new3"
    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders3"
    val trainSetUsersPath = hdfsPath + "data/train/userpay/"


    //所有用户id的dataframe  Hisense data
    val allUserPath = hdfsPath + "data/train/userpay/allUsers/user_id.txt"
    val df_allUsers = spark.read.format("csv").load(allUserPath).toDF(Dic.colUserId)

//    val allUsersSavePath = hdfsPath + "data/train/common/processed/userpay/all_users"
//    val df_allUsers = getData(spark, allUsersSavePath)
    printDf("全部用户: ", df_allUsers)


    /**
     * 训练集正负样本选取
     */
    val timeLength = 14
    val df_orders = getData(spark, ordersProcessedPath)
    val df_allTrainUsers = getTrainSetUsers(df_allUsers, df_orders, trainTime, timeLength)

    printDf("TrainUsers", df_allTrainUsers)

    saveProcessedData(df_allTrainUsers, trainSetUsersPath + "trainUsers" + args(0))


  }

  def getTrainSetUsers(df_allUsers: DataFrame, df_orders: DataFrame, trainTime: String, timeLength: Int): DataFrame = {

    /**
     * 训练集正负样本选取
     * 正样本：选择在order中trainTime到 未来14天 内的成功购买用户
     * 负样本：选择正样本以外的10倍用户
     */

    /**
     * 标记金额信息异常用户 money is 0 or 1 RMB
     */

    val df_order = df_orders.withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))
    printDf("Order after sign error money ", df_order)



    val predictResourceId = Array(100201, 100202) //要预测的套餐id
    //金额异常用户
    println("package id index 0", predictResourceId(0))
    println("package id index 1", predictResourceId(1))


    val df_illegalUsers = df_order.filter(
      col(Dic.colCreationTime) >= trainTime and col(Dic.colCreationTime) < calDate(trainTime, timeLength)
        && col(Dic.colResourceType).>(0) and col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && (col(Dic.colIsMoneyError) === 1)
    ).select(Dic.colUserId).distinct()

    println("illegal Users number", df_illegalUsers.count())


    //  正样本
    var df_trainPosUsers = df_order.filter(
      col(Dic.colCreationTime) >= trainTime and col(Dic.colCreationTime) < calDate(trainTime, timeLength)
        && col(Dic.colResourceType).>(0) and col(Dic.colResourceType).<(4)
        && (col(Dic.colResourceId) === predictResourceId(0) or col(Dic.colResourceId) === predictResourceId(1))
        && col(Dic.colOrderStatus).>(1)
    ).select(Dic.colUserId).distinct()


    //去掉金额异常用户
    df_trainPosUsers = df_trainPosUsers.except(df_illegalUsers.select(Dic.colUserId))
      .withColumn(Dic.colOrderStatus, lit(1))

    printDf("trainPosUsers", df_trainPosUsers)

    //负样本
    val df_allNegUsers = df_allUsers.except(df_trainPosUsers.select(Dic.colUserId))
      .except(df_illegalUsers) //except 保证column一致

    val df_trainNegUsers = df_allNegUsers.sample(1).limit(10 * df_trainPosUsers.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))
    printDf("df_trainNegUsers", df_trainNegUsers)

    df_trainPosUsers.union(df_trainNegUsers).distinct().sample(1)

  }


}
