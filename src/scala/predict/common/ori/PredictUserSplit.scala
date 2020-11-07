package predict.common.ori

import mam.Dic
import mam.Utils.calDate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object PredictUserSplit {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("PredictUserSplit")
      //.master("local[6]")
      .getOrCreate()
    import org.apache.spark.sql.functions._

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""

    val playsProcessedPath=hdfsPath+"data/predict/common/processed/plays"
    val ordersProcessedPath=hdfsPath+"data/predict/common/processed/orders"
    //老用户名单保存路径
    val oldUserSavePath =hdfsPath+"data/predict/userpay/"
    //新用户名单保存路径
    val newUserSavePath = hdfsPath+"data/predict/userpay/"
    val predictTime=args(0)+" "+args(1)
    val timeWindow=30

    val play = spark.read.format("parquet").load(playsProcessedPath)
    //所有用户id的列表
    val allUsersList=play.select(col(Dic.colUserId)).distinct().collect().map(_(0)).toList
    //所有用户id的dataframe
    println("用户总人数："+allUsersList.length)
    val allUsersDataFrame=play.select(col(Dic.colUserId)).distinct()




    val orderAll = spark.read.format("parquet").load(ordersProcessedPath)
    // 选择套餐订单
    val orderPackage=orderAll
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4)
      )


    //order中在train_time后14天内的支付成功订单
    val predictTimePost14=calDate(predictTime,days = 14)
    val trainPos=orderPackage
      .filter(
        col(Dic.colOrderStatus).>(1)
          && col(Dic.colCreationTime).>=(predictTime)
          && col(Dic.colCreationTime).<(predictTimePost14)
      )

    val predictTimePre=calDate(predictTime,days = -timeWindow)
    var predictOrderOld=orderPackage
      .filter(
        col(Dic.colOrderStatus).>(1)
          && ((col(Dic.colCreationTime).>(predictTimePre) && col(Dic.colCreationTime).<(predictTime))
          || (col(Dic.colOrderEndTime).>(predictTime) && col(Dic.colCreationTime).<(predictTime)) )
      )
    val joinKeysUserId=Seq(Dic.colUserId)
    predictOrderOld=allUsersDataFrame.join(predictOrderOld,joinKeysUserId,"inner")
    val predictOld=predictOrderOld.select(col(Dic.colUserId)).distinct()
    val predictNew=allUsersDataFrame.except(predictOld)
    //println("预测数据集生成完成！")
    println("需要预测的老用户的数量："+predictOld.count())
    println("需要预测的新用户的数量："+predictNew.count())
    predictOld.write.mode(SaveMode.Overwrite).format("parquet").save(oldUserSavePath+"predictusersold"+args(0))
    predictNew.write.mode(SaveMode.Overwrite).format("parquet").save(oldUserSavePath+"predictusersnew"+args(0))


  }

}
