package predict.userpay


import mam.Dic
import mam.Utils.printDf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object PredictSetProcessAllUsers {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("PredictSetAllUsersPredict")
      //.master("local[6]")
      .getOrCreate()

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""

    //全体用户
    val userListPath =  hdfsPath+"data/train/userpay/allUsers/user_id.txt"

    //训练集数据的保存路径
    val predictSetSavePath = hdfsPath+ "data/predict/userpay/"

    //最初生成的用户画像数据集路径
    val userProfilePlayPartPath = hdfsPath +"data/predict/common/processed/userpay/userprofileplaypart" + args(0)
    val userProfilePreferencePartPath = hdfsPath +"data/predict/common/processed/userpay/userprofilepreferencepart" + args(0)
    val userProfileOrderPartPath = hdfsPath +"data/predict/common/processed/userpay/userprofileorderpart" + args(0)


    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)

    val joinKeysUserId = Seq(Dic.colUserId)
    val temp=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    val userProfiles=temp.join(userProfileOrderPart,joinKeysUserId,"left")

    val userList = spark.read.format("csv").load(userListPath).toDF(Dic.colUserId)
    printDf("全部用户", userList)

    val predictSet = userList.join(userProfiles,joinKeysUserId,"left")

    val colList=predictSet.columns.toList
    val colTypeList=predictSet.dtypes.toList
    val mapColList=ArrayBuffer[String]()
    for(elem<- colTypeList){
      if(!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")){
        mapColList.append(elem._1)
      }
    }
    val numColList=colList.diff(mapColList)
    val tempPredictSet=predictSet.na.fill(-1,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
    val trainSetNotNull=tempPredictSet.na.fill(0,numColList)


    val videoFirstCategoryTempPath = hdfsPath +"data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath +"data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath +"data/train/common/processed/labeltemp.txt"
    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()



    var videoFirstCategory =spark.read.format("csv").load(videoFirstCategoryTempPath)
    var conList=videoFirstCategory.collect()
    for(elem <- conList){
      var s = elem.toString()
      videoFirstCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }

    var videoSecondCategory =spark.read.format("csv").load(videoSecondCategoryTempPath)
    conList=videoSecondCategory.collect()
    for(elem <- conList){
      var s=elem.toString()
      videoSecondCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }

    var label=spark.read.format("csv").load(labelTempPath)
    conList=label.collect()
    for(elem <- conList){
      var s=elem.toString()
      labelMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }


    val pre=List(Dic.colVideoOneLevelPreference,Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference,Dic.colSingleTwoLevelPreference,Dic.colInPackageVideoTwoLevelPreference)

    var tempDataFrame=trainSetNotNull
    //tempDataFrame.show()
    def udfFillPreference=udf(fillPreference _)
    def fillPreference(prefer:Map[String,Int],offset:Int)={
      if(prefer==null){
        null
      }else{
        val mapArray=prefer.toArray
        if (mapArray.length>offset-1){
          mapArray(offset-1)._1
        }else{
          null
        }

      }

    }

    for(elem<-pre){
      tempDataFrame=tempDataFrame.withColumn(elem+"_1",udfFillPreference(col(elem),lit(1)))
        .withColumn(elem+"_2",udfFillPreference(col(elem),lit(2)))
        .withColumn(elem+"_3",udfFillPreference(col(elem),lit(3)))
    }
    // tempDataFrame.show()
    //tempDataFrame.filter(!isnull(col("video_one_level_preference_1"))).show()
    def udfFillPreferenceIndex=udf(fillPreferenceIndex _)
    def fillPreferenceIndex(prefer:String,mapLine:String)={
      if(prefer==null){
        null
      }else{
        var tempMap: Map[String, Int] = Map()
        var lineIterator1 = mapLine.split(",")
        //迭代打印所有行
        lineIterator1.foreach(m=>tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
        tempMap.get(prefer)
      }
    }

    for(elem<-pre){
      if(elem.contains(Dic.colVideoOneLevelPreference)){
        tempDataFrame=tempDataFrame.withColumn(elem+"_1",udfFillPreferenceIndex(col(elem+"_1"),lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem+"_2",udfFillPreferenceIndex(col(elem+"_2"),lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem+"_3",udfFillPreferenceIndex(col(elem+"_3"),lit(videoFirstCategoryMap.mkString(","))))
      }else{
        tempDataFrame=tempDataFrame.withColumn(elem+"_1",udfFillPreferenceIndex(col(elem+"_1"),lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem+"_2",udfFillPreferenceIndex(col(elem+"_2"),lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem+"_3",udfFillPreferenceIndex(col(elem+"_3"),lit(videoSecondCategoryMap.mkString(","))))
      }
    }
    //tempDataFrame.filter(!isnull(col("video_one_level_preference_1"))).show()
    for(elem<-pre){
      if(elem.equals(Dic.colVideoOneLevelPreference)){
        tempDataFrame=tempDataFrame.na.fill(videoFirstCategoryMap.size,List(elem+"_1",elem+"_2",elem+"_3"))
      }else{
        tempDataFrame=tempDataFrame.na.fill(videoSecondCategoryMap.size,List(elem+"_1",elem+"_2",elem+"_3"))
      }

    }
    //tempDataFrame.filter(col("video_one_level_preference_1")=!=13).show()
    val columnTypeList=tempDataFrame.dtypes.toList
    val columnList=ArrayBuffer[String]()
    for(elem<- columnTypeList){
      if(elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")){
        columnList.append(elem._1)
      }
    }

    val result=tempDataFrame.select(columnList.map(tempDataFrame.col(_)):_*)

    result.write.mode(SaveMode.Overwrite).option("header","true").csv(predictSetSavePath + "predictSet" + args(0)+".csv")

  }

}
