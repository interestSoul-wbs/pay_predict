package predict.singlepoint

import mam.Dic
import mam.Utils.printDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object RankPredictDatasetGenerate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("RankPredictDatasetGenerate")
      //.master("local[6]")
      .config("spark.sql.crossJoin.enabled","true")  //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val ordersProcessedPath=hdfsPath+"data/predict/common/processed/orders"
    val orders=spark.read.format("parquet").load(ordersProcessedPath)

    val userProfilePlayPartPath=hdfsPath+"data/predict/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/predict/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/predict/common/processed/userprofileorderpart"+args(0)
    val videoProfilePath=hdfsPath+"data/predict/common/processed/videoprofile"+args(0)
    val videoVectorPath=hdfsPath+"data/predict/common/processed/videovector"+args(0)
    val userDivisionResultPath=hdfsPath+"data/predict/singlepoint/userdivisionresult"+args(0)+"-"+args(2)

    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val userDivisionResult=spark.read.format("parquet").load(userDivisionResultPath)
    val videoProfile=spark.read.format("parquet").load(videoProfilePath)
    val videoVector=spark.read.format("parquet").load(videoVectorPath)

    printDf("userDivisionResult",userDivisionResult)
    printDf("userProfilePlayPart",userProfilePlayPart)
    printDf("userProfilePreferencePart",userProfilePreferencePart)
    printDf("userProfileOrderPart",userProfileOrderPart)
    printDf("videoProfile",videoProfile)
    printDf("videoVector",videoVector)
    printDf("orders",orders)


    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)

    var userProfile=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    userProfile=userProfile.join(userProfileOrderPart,joinKeysUserId,"left")
    //由于内存的限制，设置预测的单点视频的数量，如果内存足够大可以将单点视频的数量设置为媒资中所有单点视频的数量
    val selectN=20
    var selectSinglePoint=orders.filter(
      col(Dic.colResourceType).===(0)
      && col(Dic.colCreationTime).<(args(0))
      && col(Dic.colOrderStatus).>(1)
    ).groupBy(col(Dic.colResourceId))
      .agg(count(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(selectN)
      .select(col(Dic.colResourceId))
      .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    var selectVideos=selectSinglePoint.join(videoProfile,joinKeysVideoId,"inner")
    //之前考虑只将订单中销量最好的单点视频推荐给用户，现修改为将媒资数据中的
//    val selectVideos=videoProfile.filter(col(Dic.colIsSingle).===(1))
    //println("预测的单点视频的数量："+selectVideos.count())

    var selectUsers=userDivisionResult.select(col(Dic.colUserId)).join(userProfile,joinKeysUserId,"inner")
    println("预测的用户的数量："+selectUsers.count())
    var result=selectUsers.crossJoin(selectVideos)
    result=result.join(videoVector,joinKeysVideoId,"left")
    //println("预测的数据的条数："+result.count())



    val colTypeList=result.dtypes.toList
    val colList=ArrayBuffer[String]()
    colList.append(Dic.colUserId)
    colList.append(Dic.colVideoId)
    for(elem<- colTypeList){
      if(elem._2.equals("IntegerType") || elem._2.equals("DoubleType") || elem._2.equals("LongType")){
        colList.append(elem._1)
      }
    }
    colList-=Dic.colIsSingle
    colList-=Dic.colIsTrailers
    colList-=Dic.colIsPaid
    //colList-=Dic.colVideoTime
    //colList-=Dic.colScore
    //    colList.foreach(println)
    //    println(colList.length)
    val seqColList=colList.toSeq
    result=result.select(seqColList.map(result.col(_)):_*)
    result=result.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
    result=result.na.fill(0)
    //result.show()
    println("总样本的条数"+result.count())

    printDf("result",result)

    val resultSavePath=hdfsPath+"data/predict/singlepoint/rankpredictdata"
    result.write.mode(SaveMode.Overwrite).format("parquet").save(resultSavePath+args(0)+"-"+args(2))
    val csvData=spark.read.format("parquet").load(resultSavePath+args(0)+"-"+args(2))
    csvData.write.mode(SaveMode.Overwrite).option("header","true").csv(resultSavePath+args(0)+"-"+args(2)+".csv")




  }

}
