package predict.singlepoint

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getProcessedOrder, getUserProfileOrderPart, getUserProfilePlayPart, getUserProfilePreferencePart, saveSinglePointPredictUsers, saveSinglePointTrainUsers, scaleData}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting, udfAddOrderStatus}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, count, explode, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


import scala.collection.mutable.ArrayBuffer

object UserDivisionPredictDatasetGenerate {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()

    // 2 Get Data
    val timeWindowStart=args(0)+" "+args(1)
    val timeWindowEnd=args(2)+" "+args(3)

    val userProfileOrderPart=getUserProfileOrderPart(spark,timeWindowStart,"predict")
    val userProfilePlayPart=getUserProfilePlayPart(spark,timeWindowStart,"predict")
    val userProfilePreferencePart=getUserProfilePreferencePart(spark,timeWindowStart,"predict")

    val joinKeysUserId = Seq(Dic.colUserId)
    val userProfiles=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
      .join(userProfileOrderPart,joinKeysUserId,"left")
    printDf("输入 userProfiles",userProfiles)
    //3 Process Data
    val df_predict_users=processPredictUsers(userProfiles)
    printDf("输出 df_predict_users",df_predict_users)


    //4 Save Data
    saveSinglePointPredictUsers(timeWindowStart,timeWindowEnd,df_predict_users)
    println("UserDivisionPredictDatasetGenerate  over~~~~~~~~~~~")





  }

  def  processPredictUsers(userProfiles:DataFrame)={
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
    val allUsersNotNull=userProfiles.select(seqColList.map(userProfiles.col(_)):_*)
      .na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
      .na.fill(0)
      .na.drop()

    val exclude_cols = Array(Dic.colUserId)
    val df_result = scaleData(allUsersNotNull, exclude_cols)
    println("预测的用户总数为："+df_result.count())
    df_result

  }



}
