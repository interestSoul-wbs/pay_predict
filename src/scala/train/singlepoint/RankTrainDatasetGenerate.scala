package train.singlepoint

import mam.Dic
import mam.Utils
import mam.Utils.{calDate, udfAddOrderStatus}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RankTrainDatasetGenerate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("RankTrainDatasetGenerate")
      //.master("local[6]")
      .config("spark.sql.crossJoin.enabled","true")  //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
    val orders=spark.read.format("parquet").load(ordersProcessedPath)

    val userProfilePlayPartPath=hdfsPath+"data/train/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/train/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/train/common/processed/userprofileorderpart"+args(0)
    val videoProfilePath=hdfsPath+"data/train/common/processed/videoprofile"+args(0)
    val videoVectorPath=hdfsPath+"data/train/common/processed/videovector"+args(0)
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val videoProfile=spark.read.format("parquet").load(videoProfilePath)
    val videoVector=spark.read.format("parquet").load(videoVectorPath)




    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)

    var userProfile=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    userProfile=userProfile.join(userProfileOrderPart,joinKeysUserId,"left")


    val predictWindowStart=args(0)+" "+args(1)
    val predictWindowEnd=args(2)+" "+args(3)

    val trainWindowStart=calDate(predictWindowStart,-30 )
    //println(trainWindowStart)


    //在order订单中选出正样本
    val orderSinglePoint=orders
      .filter(
        col(Dic.colResourceType).===(0)
        && col(Dic.colCreationTime).>=(predictWindowStart)
        && col(Dic.colCreationTime).<=(predictWindowEnd)
        && col(Dic.colOrderStatus).>(1)
      ).select(col(Dic.colUserId),col(Dic.colResourceId),col(Dic.colOrderStatus))
      .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    var dataset1=orderSinglePoint.join(userProfile,joinKeysUserId,"inner")
    dataset1=dataset1.join(videoProfile,joinKeysVideoId,"inner")
    println("第一部分数据条数："+dataset1.count())
    //temp.show()




    //第一部分的负样本
    val temp=dataset1.select(col(Dic.colUserId),col(Dic.colVideoId))
    //设置负样本中选择多少个video作为负样本中的video
    val negativeN=10
    val popularVideo=temp.groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))
    var dataset2=temp.select(col(Dic.colUserId)).distinct().crossJoin(popularVideo).except(temp)
    dataset2=dataset2.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    dataset2=dataset2.join(userProfile,joinKeysUserId,"inner")
    dataset2=dataset2.join(videoProfile,joinKeysVideoId,"inner")
    println("第二部分数据条数："+dataset2.count())
   // dataset2.show()


    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val negativeUserN=10*temp.select(col(Dic.colUserId)).distinct().count()
    val negativeUsers=userProfile.select(col(Dic.colUserId)).except(temp.select(col(Dic.colUserId))).limit(negativeUserN.toInt)
    //val negativeVideos=temp.select(col(Dic.colVideoId)).distinct()
    var dataset3=negativeUsers.crossJoin(popularVideo)//.sample(false,1/negativeVideos.count())
    dataset3=dataset3.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    println("第三部分数据条数："+dataset3.count())

    dataset3=dataset3.join(userProfile,joinKeysUserId,"inner")
    dataset3=dataset3.join(videoProfile,joinKeysVideoId,"inner")
    //dataset3.show()
    var result=dataset1.union(dataset2)//.union(dataset3)
    result=result.join(videoVector,joinKeysVideoId,"left")


//    val seq=mapColList.toSeq
//    result.select(seq.map(result.col(_)):_*).show()

    val videoFirstCategoryTempPath=hdfsPath+"data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath=hdfsPath+"data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath=hdfsPath+"data/train/common/processed/labeltemp.txt"
    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()



    var videoFirstCategory =spark.read.format("csv").load(videoFirstCategoryTempPath)
    var conList=videoFirstCategory.collect()
    for(elem <- conList){
      var s=elem.toString()
      videoFirstCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }
    //println(videoFirstCategoryMap)
    var videoSecondCategory =spark.read.format("csv").load(videoSecondCategoryTempPath)
    conList=videoSecondCategory.collect()
    for(elem <- conList){
      var s=elem.toString()
      videoSecondCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }
    //println(videoSecondCategoryMap)
    var label=spark.read.format("csv").load(labelTempPath)
    conList=label.collect()
    for(elem <- conList){
      var s=elem.toString()
      labelMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }

//    val aMap:Map[String,Int]=Map()
//    for(elem<-mapString.split(",")){
//      aMap+=(elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
//    }
//    //mapString.split(",").foreach(item=>labelMap+=(item.split(" -> ")(0) -> item.split(" -> ")(1).toInt))
//    println(aMap)





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
    //colList-=Dic.colPackageId
    //colList.foreach(println)
    //println(colList.length)
    val seqColList=colList.toSeq
    result=result.select(seqColList.map(result.col(_)):_*)
    result=result.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
    result=result.na.fill(0)
    //result.show()
    println("总样本的条数"+result.count())

    val resultSavePath=hdfsPath+"data/train/singlepoint/ranktraindata"
    result.write.mode(SaveMode.Overwrite).format("parquet").save(resultSavePath+args(0)+"-"+args(2))
    val csvData=spark.read.format("parquet").load(resultSavePath+args(0)+"-"+args(2))
    csvData.write.mode(SaveMode.Overwrite).option("header","true").csv(resultSavePath+args(0)+"-"+args(2)+".csv")
    //result.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "Example").save(resultSavePath+args(0)+"-"+args(2)+".tfrecords")
  }

}
