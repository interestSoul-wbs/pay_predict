package train.singlepoint

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedOrder, getUserProfileOrderPart, getUserProfilePlayPart, getUserProfilePreferencePart, saveSinglePointTrainUsers, scaleData}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting, udfAddOrderStatus, udfGetString}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.TimeWindow
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object UserDivisionTrainDatasetGenerate {



  def main(args:Array[String]): Unit ={

    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val timeWindowStart=args(0)+" "+args(1)
    val timeWindowEnd=args(2)+" "+args(3)
    val df_orders=getProcessedOrder(spark)
    printDf("输入 df_order",df_orders)
    val userProfileOrderPart=getUserProfileOrderPart(spark,timeWindowStart,"train")
    val userProfilePlayPart=getUserProfilePlayPart(spark,timeWindowStart,"train")
    val userProfilePreferencePart=getUserProfilePreferencePart(spark,timeWindowStart,"train")

    val joinKeysUserId = Seq(Dic.colUserId)
    val userProfiles=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
      .join(userProfileOrderPart,joinKeysUserId,"left")
    printDf("输入 userProfiles",userProfiles)
    //3 Process Data
    val df_result=processTrainUsers(df_orders,userProfiles,timeWindowStart,timeWindowEnd)
    printDf("输出  df_result", df_result)
    //4 Save Data
    saveSinglePointTrainUsers(timeWindowStart,timeWindowEnd,df_result)

    println("UserDivisionTrainDatasetGenerate  over~~~~~~~~~~~")


  }


  def   processTrainUsers(df_orders:DataFrame,userProfiles:DataFrame,timeWindowStart:String,timeWindowEnd:String)={
    //在用来训练的时间窗口内的单点视频的订单
    val singlePaidOrders=df_orders.filter(
      col(Dic.colCreationTime).>=(timeWindowStart)
        && col(Dic.colCreationTime).<=(timeWindowEnd)
        && col(Dic.colResourceType).===(0)
        && col(Dic.colOrderStatus).>(1)
    )
    //过滤掉偏好等不需要输入模型中的信息
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
    val joinKeysUserId = Seq(Dic.colUserId)
    val usersPaidProfile=userProfiles
      .join(singlePaidOrders,joinKeysUserId,"inner")
      .select(seqColList.map(userProfiles.col(_)):_*)
      .dropDuplicates(Dic.colUserId)
    //usersPaidProfile.show()
    println("正样本的条数为："+usersPaidProfile.count())
    val positiveCount:Int=usersPaidProfile.count().toInt
    //构造负样本，确定正负样本的比例为1:10，不同的负样本的比例对于AUC指标的影响不大，但是对于召回的结果影响比较大。
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
    //填补缺失值
    val allUsersNotNull=allUsers.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()
    //对数据进行归一化
    val exclude_cols = Array(Dic.colUserId)
    val df_result = scaleData(allUsersNotNull, exclude_cols)
    df_result
  }

}
