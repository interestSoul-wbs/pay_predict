package train.userpay

/**
 * @author wx
 * @describe order中的订单历史，生成过去一个月的消费历史：包含支付成功和未支付成功
 */
import mam.Dic
import mam.Utils.{calDate, printDf, udfGetDays}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, collect_list, desc, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrderHistoryProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("OrderHistory")
      .master("local[6]")
      .getOrCreate()


    val hdfsPath = ""
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders"
    //val hdfsPath = "hdfs:///pay_predict/"
    //val orderProcessedPath = hdfsPath + "data/train/common/processed/userpay/order_new"
    val allUsersSavePath = hdfsPath + "data/train/common/processed/userpay/all_users/all_users.csv"


    //val now = args(0)+" "+args(1)
    val now = "2020-04-23 00:00:00"
    val timeWindow = 30

    //play数据获取
    var order = getData(spark, orderProcessedPath)
    printDf("order", order)

    val trainOrders = getTrainOrders(order, 30, now)

    printDf("train_order", trainOrders)

  }

  def getData(spark: SparkSession, path: String) = {
    /**
     * @author wj
     * @param [spark, orderProcessedPath]
     * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     * @description 读取数据
     */
    spark.read.format("parquet").load(path)
  }


  def getTrainOrders(orderDf: DataFrame, timeWindow: Int, now: String): DataFrame = {
    /**
     * @describe
     * @author wx
     * @param [mediasDf]
     * @param [timeWindow]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset< org.apache.spark.sql.Row > }
     * */

    orderDf.filter(col(Dic.colCreationTime).<(now) and col(Dic.colCreationTime) >= calDate(now, days = -timeWindow))
      .withColumn("now", lit(now))
      .withColumn(Dic.colCreationGap, udfGetDays(col(Dic.colCreationTime), col("now")))
      .orderBy(desc(Dic.colCreationTime))
      .drop("now")
      .select(Dic.colUserId, Dic.colMoney, Dic.colResourceType, Dic.colCreationGap, Dic.colTimeValidity, Dic.colOrderStatus)
  }


}