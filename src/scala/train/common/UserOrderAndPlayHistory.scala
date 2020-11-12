package train.common

import mam.Dic
import mam.Utils.{calDate, printDf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * @author wj
 * @date 2020/11/6
 * @version 0.1
 * @describe   收集最近一段时间用户的播放历史和全部的单点订单的历史
 */
object UserOrderAndPlayHistory {
  def main(args: Array[String]): Unit = {
    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath=""
    val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
    val now=args(0)+" "+args(1)

    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserOrderAndPlayHistory")
      .master("local[6]")
      .getOrCreate()


    //val medias = spark.read.format("parquet").load(medias_path)
    val plays = getPlays(playsProcessedPath,spark)
    val orders = getOrders(ordersProcessedPath,spark)
    printDf("输入 plays",plays)
    printDf("输入 orders",orders)

    val orderList=getUserOrdersList(orders,now)
    val playList=getUserPlaysList(plays,now)
    printDf("输出 orderList",orderList)
    printDf("输出 playList",playList)
    val orderListSavePath=hdfsPath+"data/train/common/processed/orderList"+args(0)
    val playListSavePath=hdfsPath+"data/train/common/processed/playList"+args(0)
    saveOrderList(orderList,orderListSavePath)
    savePlayList(playList,playListSavePath)






  }
  def getOrders(ordersProcessedPath:String,spark:SparkSession)={
    spark.read.format("parquet").load(ordersProcessedPath)
  }
  def getPlays(playsProcessedPath:String,spark:SparkSession)={
    spark.read.format("parquet").load(playsProcessedPath)
  }
  def getUserPlaysList(plays:DataFrame,now:String)={
    //选取最近一周的观看历史
    println(calDate(now,-7))
    val playsSelect=plays.filter(
      col(Dic.colPlayEndTime).<(now)
      && col(Dic.colPlayEndTime).>(calDate(now,-7))
    ).withColumn("row_number",
      row_number().over(Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colPlayEndTime).desc,col(Dic.colBroadcastTime).desc)))
    playsSelect.show()
    //平均每个用户每周会看13个视频
    playsSelect.groupBy(col(Dic.colUserId)).agg(collect_list(col(Dic.colVideoId)).as("play_list"))
  }
  def getUserOrdersList(orders:DataFrame,now:String)={

    val orderSinglePoint=orders.filter(
      col(Dic.colOrderStartTime).<(now)
      && col(Dic.colOrderStatus).>(1)
      && col(Dic.colResourceType).===(0)
    )
  //orderSinglePoint.withColumn("row_number",row_number().over(Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colCreationTime).desc))).show()
    orderSinglePoint.groupBy(col(Dic.colUserId)).agg(collect_list(col(Dic.colResourceId)).as("order_list"))


  }
  def saveOrderList(orderList:DataFrame,savePath:String)={
    orderList.write.mode(SaveMode.Overwrite).format("parquet").save(savePath)

  }
  def savePlayList(playList:DataFrame,savePath:String)={
    playList.write.mode(SaveMode.Overwrite).format("parquet").save(savePath)

  }

}
