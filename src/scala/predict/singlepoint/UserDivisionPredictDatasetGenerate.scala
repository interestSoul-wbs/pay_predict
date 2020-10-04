package predict.singlepoint

import mam.Dic
import mam.Utils.udfAddOrderStatus
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object UserDivisionPredictDatasetGenerate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val userProfilePlayPartPath=hdfsPath+"data/predict/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/predict/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/predict/common/processed/userprofileorderpart"+args(0)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserDivisionPredictDatasetGenerate")
      //.master("local[6]")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)

    val joinKeysUserId = Seq(Dic.colUserId)
    var userProfiles=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
     userProfiles=userProfiles.join(userProfileOrderPart,joinKeysUserId,"left")
    var userProfilesTest=userProfiles
    //过滤掉偏好
    val colTypeList=userProfiles.dtypes.toList
    val colList=ArrayBuffer[String]()
    for(elem<- colTypeList){
      if(elem._2.equals("IntegerType") || elem._2.equals("DoubleType")
        || elem._2.equals("LongType") || elem._2.equals("StringType")){
        colList.append(elem._1)
      }
    }
    val seqColList=colList.toSeq
    userProfiles=userProfiles.select(seqColList.map(userProfiles.col(_)):_*)
    userProfiles=userProfiles.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
    userProfiles=userProfiles.na.fill(0)
    userProfiles=userProfiles.na.drop()
    val predictDataSavePath=hdfsPath+"data/predict/singlepoint/userdivisionpredictdata"
    userProfiles.write.mode(SaveMode.Overwrite).format("parquet").save(predictDataSavePath+args(0)+"-"+args(2))
    userProfiles.write.mode(SaveMode.Overwrite).option("header","true").csv(predictDataSavePath+args(0)+"-"+args(2)+".csv")





    val orderProcessedPath=hdfsPath+"data/predict/common/processed/orders"
    val orders = spark.read.format("parquet").load(orderProcessedPath).toDF()
    val predictWindowStart=args(0)+" "+args(1)
    val predictWindowEnd=args(2)+" "+args(3)
    //在预测时间窗口内的单点视频的订单
    val singlePaidOrders=orders.filter(
      col(Dic.colCreationTime).>=(predictWindowStart)
        && col(Dic.colCreationTime).<=(predictWindowEnd)
        && col(Dic.colResourceType).===(0)
        && col(Dic.colOrderStatus).>(1)
    )

    //找出订购了单点视频的用户的用户画像作为正样本

    val usersPaidProfile=userProfilesTest
      .join(singlePaidOrders, joinKeysUserId, "inner")
      .select(seqColList.map(userProfilesTest.col(_)): _*)
      .dropDuplicates(Dic.colUserId)
    //usersPaidProfile.show()
    println("测试正样本的条数为："+usersPaidProfile.count())
    val positiveCount:Int=usersPaidProfile.count().toInt
    //构造负样本，确定正负样本的比例为1:10
    val NEGATIVE_N:Int=10
    val negativeUsers=userProfilesTest.select(seqColList.map(userProfilesTest.col(_)):_*)
      .except(usersPaidProfile).sample(fraction = 1.0).limit(NEGATIVE_N*positiveCount)
    println("测试负样本的条数为："+negativeUsers.count())
    //为正负样本分别添加标签
    val negativeUsersWithLabel=negativeUsers.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    val usersPaidWithLabel=usersPaidProfile.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))
    //将正负样本组合在一起并shuffle
    var allUsers=usersPaidWithLabel.union(negativeUsersWithLabel).sample(fraction = 1.0)
    println("测试总样本的条数为："+allUsers.count())

    allUsers=allUsers.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
    allUsers=allUsers.na.fill(0)
    allUsers=allUsers.na.drop()

    val testDataSavePath=hdfsPath+"data/predict/singlepoint/userdivisiontestdata"
   allUsers.write.mode(SaveMode.Overwrite).format("parquet").save(testDataSavePath+args(0)+"-"+args(2))
    allUsers.write.mode(SaveMode.Overwrite).option("header","true").csv(testDataSavePath+args(0)+"-"+args(2)+".csv")


    //allUsersNotNull=allUsersNotNull.na.drop()



  }

}
