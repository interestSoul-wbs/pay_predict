package train.userpay

import mam.Dic
import mam.Utils.printDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @author wx
 * @param
 * @return
 * @describe 获取play和orders中的全部用户
 */
object GetAllUsers {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("getAllUsersForUserpay")
      .master("local[6]")
      .getOrCreate()

    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val orderProcessedPath = hdfsPath + "data/train/common/processed/userpay/orders"
    val playProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new"
    val allUsersSavePath = hdfsPath + "data/train/common/processed/userpay/all_users"


    val ordersUsers = getOrdersUsers(orderProcessedPath, spark)
    printDf("得到 orderUsers", ordersUsers)

    val playsUsers = getPlaysUsers(playProcessedPath, spark)
    printDf("得到 playsUsers", playsUsers)

    val allUsers = getAllUsers(ordersUsers, playsUsers)
    //allUsers.write.mode(SaveMode.Overwrite).format("parquet").save(allUsersSavePath)

    allUsers.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(allUsersSavePath)

    println("用户获取完毕！")
  }

    def getOrdersUsers(orderProcessedPath: String, spark: SparkSession):DataFrame = {
      val schema = StructType(
        List(
          StructField(Dic.colUserId, StringType),
          StructField(Dic.colMoney, DoubleType),
          StructField(Dic.colResourceType, StringType),
          StructField(Dic.colResourceId, StringType),
          StructField(Dic.colResourceTitle, StringType),
          StructField(Dic.colCreationTime, StringType),
          StructField(Dic.colDiscountDescription, StringType),
          StructField(Dic.colOrderStatus, DoubleType),
          StructField(Dic.colOrderStartTime, StringType),
          StructField(Dic.colOrderEndTime, StringType),
          StructField(Dic.colTimeValidity, IntegerType),
          StructField(Dic.colIsMoneyError, IntegerType)
        )
      )
      val df = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .schema(schema)
        .csv(orderProcessedPath)

      val user = df.select(Dic.colUserId).distinct()
      user
    }

    def getPlaysUsers(playProcessedPath: String, spark: SparkSession):DataFrame = {
      val schema = StructType(
        List(
          StructField(Dic.colUserId, StringType),
          StructField(Dic.colPlayStartTime, StringType),
          StructField(Dic.colVideoId, StringType),
          StructField(Dic.colTimeSum, FloatType)
        )
      )
      val df = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .schema(schema)
        .csv(playProcessedPath)

      val user = df.select(Dic.colUserId).distinct()
      user
    }

    def getAllUsers(ordersUsers:DataFrame, playsUsers:DataFrame): DataFrame ={

      val allUsers = ordersUsers.union(playsUsers).distinct()

      allUsers

    }







}
