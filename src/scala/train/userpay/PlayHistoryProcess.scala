package train.userpay
/**
 * @author wx
 * @describe 用户播放历史
 */
import java.text.SimpleDateFormat
import java.util

import mam.Dic
import mam.Utils.{calDate, printDf, udfBreak}
import org.apache.arrow.flatbuf.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.catalyst.expressions.TimeWindow
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.functions.{col, collect_list, lit, substring}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

object PlayHistoryProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("PlayHistory")
      .master("local[6]")
      .getOrCreate()


    val hdfsPath=""
    val playsProcessedPath = hdfsPath + "data/train/common/processed/plays"
    //val hdfsPath = "hdfs:///pay_predict/"
    //val playsProcessedPath = hdfsPath + "data/train/common/processed/userpay/plays_new"
    val allUsersSavePath = hdfsPath + "data/train/common/processed/userpay/all_users/all_users.csv"


    //val now = args(0)+" "+args(1)
    val now = "2020-04-23 00:00:00"
    val timeWindow = 14

    //play数据获取
    var plays = getData(spark, playsProcessedPath)
    printDf("plays", plays)

    //放在HDFS记得删掉
    plays = plays.withColumnRenamed(Dic.colPlayEndTime, Dic.colPlayStartTime)

    // 获取HDFS中的allUsers
    //val allUsers = getData(spark, allUsersSavePath)
    //printDf("allUsers", allUsers)

    //获取csv文件格式 allUsers
    val allUsers = getCsv(spark, allUsersSavePath)
    printDf("allUsers CSV", allUsers)


    /**
     * 对于每个用户生成播放历史，14天内的播放历史，取n条，截取和补0
     */

    val playsVideoList = getBehaviorSequence(plays, Dic.colVideoId, now, timeWindow)
    val playsTimeList = getBehaviorSequence(plays, Dic.colBroadcastTime, now, timeWindow)

    val playsList = playsVideoList.join(playsTimeList, Dic.colUserId)

    printDf("plays_list",playsList)


  }
  def getData(spark:SparkSession,path:String)={
    /**
     *@author wj
     *@param [spark, playsProcessedPath]
     *@return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
     *@description 读取数据
     */
    spark.read.format("parquet").load(path)
  }

  def getCsv(spark: SparkSession, path: String):DataFrame = {
    /**
     * @describe 读取csv文件
     * @author wx
     * @param [spark] sparkSession
     * @param [path] 存储路径
     * @return {@link org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> }
     **/
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("nullValue", "\\N")
      .option("inferSchema", "true") //这是自动推断属性列的数据类型
      .load(path)
      //.toDF("user_id")

    data
  }

  def getBehaviorSequence(playDf:DataFrame,colName:String, now:String, timeWindow: Int) ={
    /**
     * @describe 按照userid和播放起始时间逆向排序 选取 now - timewindow 到 now的播放历史和播放时长
     * @author wx
     * @param [plays]
     * @param [now]
     * @return {@link org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> }
     **/

    val playList = playDf.filter(col(Dic.colPlayStartTime).<(now) && col(Dic.colPlayStartTime) >= calDate(now, days = -timeWindow))
                        .orderBy(Dic.colUserId, Dic.colPlayStartTime)
                        .groupBy(col(Dic.colUserId))
                        .agg(collect_list(col(colName)).as(colName+"_list"))
    playList
  }

}
