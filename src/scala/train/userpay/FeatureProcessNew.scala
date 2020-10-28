package train.userpay

import mam.Dic
import mam.Utils
import mam.Utils.{printDf, udfFillPreference, udfFillPreferenceIndex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

object FeatureProcessNew {



  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    //val userProfilePath =  "pay_predict/data/train/common/processed/userfile_0601.pkl"
    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    //筛选的训练集用户名单路径
    val userListPath = hdfsPath+"data/train/userpay/trainusersnew"+args(0)
    //媒资数据路径
    val mediasPath = hdfsPath+"data/train/common/processed/mediastemp.pkl"
    //训练集数据的保存路径
    val trainSetSavePath =hdfsPath+ "data/train/userpay/"
    //最初生成的用户画像数据集路径
    val userProfilePlayPartPath=hdfsPath+"data/train/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/train/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/train/common/processed/userprofileorderpart"+args(0)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("FeatureProcessNew")
      //.master("local[6]")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val joinKeysUserId = Seq(Dic.colUserId)
    val temp=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    val userProfiles=temp.join(userProfileOrderPart,joinKeysUserId,"left")
//    val orders = spark.read.format("parquet").load(orderProcessedPath).toDF()
    val userList=spark.read.format("parquet").load(userListPath)

    printDf("userProfilePlayPart",userProfilePlayPart)
    printDf("userProfilePreferencePart",userProfilePreferencePart)
    printDf("userProfileOrderPart",userProfileOrderPart)
    printDf("userList",userList)

    val trainSet=userList.join(userProfiles,joinKeysUserId,"left")
    //trainSet.show()
    //println(trainSet.count())
    val colList=trainSet.columns.toList
    val colTypeList=trainSet.dtypes.toList
    val mapColList=ArrayBuffer[String]()
    for(elem<- colTypeList){
      if(!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")){
         mapColList.append(elem._1)
      }
    }
    val numColList=colList.diff(mapColList)
    val tempTrainSet=trainSet.na.fill(-1,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
    val trainSetNotNull=tempTrainSet.na.fill(0,numColList)

    //# 观看时长异常数据处理：1天24h



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


    for(elem<-pre){
      tempDataFrame=tempDataFrame.withColumn(elem+"_1",udfFillPreference(col(elem),lit(1)))
        .withColumn(elem+"_2",udfFillPreference(col(elem),lit(2)))
        .withColumn(elem+"_3",udfFillPreference(col(elem),lit(3)))
    }
    //tempDataFrame.show()
    //tempDataFrame.filter(!isnull(col("video_one_level_preference_1"))).show()


    for(elem<-pre){
      if(elem.equals(Dic.colVideoOneLevelPreference)){
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
    printDf("result",result)
    result.write.mode(SaveMode.Overwrite).format("parquet").save(trainSetSavePath+"trainsetnew"+args(0))
    result.write.mode(SaveMode.Overwrite).option("header","true").csv(trainSetSavePath + "trainsetnew" + args(0)+".csv")





  }

}
