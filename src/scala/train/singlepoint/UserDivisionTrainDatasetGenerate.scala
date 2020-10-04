package train.singlepoint

import mam.Dic
import mam.Utils.{udfAddOrderStatus, udfGetString}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object UserDivisionTrainDatasetGenerate {



  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val hdfsPath="hdfs:///pay_predict/"
   // val hdfsPath=""
    val orderProcessedPath=hdfsPath+"data/train/common/processed/orders"
    val userProfilePlayPartPath=hdfsPath+"data/train/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/train/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/train/common/processed/userprofileorderpart"+args(0)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserDivisionTrainDatasetGenerate")
      .master("local[6]")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val orders = spark.read.format("parquet").load(orderProcessedPath).toDF()

    val joinKeysUserId = Seq(Dic.colUserId)
    val temp=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    val userProfiles=temp.join(userProfileOrderPart,joinKeysUserId,"left")

    //println(orders.count())
   // println(userProfiles.count())

    //val predictWindowStart="2020-04-24 00:00:00"
    val predictWindowStart=args(0)+" "+args(1)
    val predictWindowEnd=args(2)+" "+args(3)
    //在预测时间窗口内的单点视频的订单
    val singlePaidOrders=orders.filter(
      col(Dic.colCreationTime).>=(predictWindowStart)
      && col(Dic.colCreationTime).<=(predictWindowEnd)
      && col(Dic.colResourceType).===(0)
      && col(Dic.colOrderStatus).>(1)
    )
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

    //找出订购了单点视频的用户的用户画像作为正样本

    val usersPaidProfile=userProfiles
      .join(singlePaidOrders,joinKeysUserId,"inner")
      .select(seqColList.map(userProfiles.col(_)):_*)
      .dropDuplicates(Dic.colUserId)
    //usersPaidProfile.show()
    println("正样本的条数为："+usersPaidProfile.count())
    val positiveCount:Int=usersPaidProfile.count().toInt
    //构造负样本，确定正负样本的比例为1:10
    val NEGATIVE_N:Int=10
    val negativeUsers=userProfiles.select(seqColList.map(userProfiles.col(_)):_*)
      .except(usersPaidProfile).sample(fraction = 1.0).limit(NEGATIVE_N*positiveCount)
    println("负样本的条数为："+negativeUsers.count())
    //为正负样本分别添加标签
    val negativeUsersWithLabel=negativeUsers.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    val usersPaidWithLabel=usersPaidProfile.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))
    //将正负样本组合在一起并shuffle
    val allUsers=usersPaidWithLabel.union(negativeUsersWithLabel).sample(fraction = 1.0)
    println("总样本的条数为："+allUsers.count())

    val tempProfile=allUsers.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
    var allUsersNotNull=tempProfile.na.fill(0)
    allUsersNotNull=allUsersNotNull.na.drop()
    //allUsersNotNull.show()
    //val columns:Array[String]=allUsersNotNull.columns
   // val col1=columns.tail.take(columns.length-2)


//    val assembler = new VectorAssembler()
//      .setInputCols(col1)
//      .setOutputCol("features_not_scale")
//    val allUsersConcat = assembler.transform(allUsersNotNull)
//    //allUsersConcat.select("features_not_scale").show()
//
//    val scaleTool=new MinMaxScaler()
//      .setInputCol("features_not_scale")
//      .setOutputCol("features_scale")
//
//    // Compute summary statistics and generate MinMaxScalerModel
//    val scaleModel = scaleTool.fit(allUsersConcat)
//
//    // rescale each feature to range [min, max].
//    val scaledData = scaleModel.transform(allUsersConcat)
    val dataPath=hdfsPath+"data/train/singlepoint/userdivisiontraindata"
    allUsersNotNull.write.mode(SaveMode.Overwrite).format("parquet").save(dataPath+args(0)+"-"+args(2))
    allUsersNotNull.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(dataPath+args(0)+"-"+args(2)+".csv")

//    val model = new LogisticRegression()  //建立模型
//    model.setLabelCol(Dic.colOrderStatus).setFeaturesCol("features_scale").fit(scaledData)
   // spark.read.format("parquet").load(dataPath+args(0)+"--"+args(2)).show()








  }

}
