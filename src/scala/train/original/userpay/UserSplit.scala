package train.original.userpay

import mam.Dic
import mam.Utils.{calDate, udfAddOrderStatus}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

object UserSplit {


  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("UserSplit")
      //.master("local[6]")
      .getOrCreate()
    import org.apache.spark.sql.functions._

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""

    val playsProcessedPath=hdfsPath+"data/train/common/processed/plays"
    val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
    //老用户名单保存路径
    val oldUserSavePath =hdfsPath+"data/train/userpay/"
    //新用户名单保存路径
    val newUserSavePath = hdfsPath+"data/train/userpay/"
    val trainTime=args(0)+" "+args(1)
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
    val trainTimePost14=calDate(trainTime,days = 14)
    val trainPos=orderPackage
      .filter(
      col(Dic.colOrderStatus).>(1)
      && col(Dic.colCreationTime).>=(trainTime)
      && col(Dic.colCreationTime).<(trainTimePost14)
    )
//    println(trainTime)
//    println(trainTimePost14)
//    println(trainPos.count())


    //println("trainPos.shape："+trainPos.count())
    //在time-time_window到time时间段内成功支付过订单 或者 在time之前创建的订单到time时仍旧有效
    val trainTimePre=calDate(trainTime,days = -timeWindow)
    var trainOrderOld=orderPackage
      .filter(
        col(Dic.colOrderStatus).>(1)
        && ((col(Dic.colCreationTime).>(trainTimePre) && col(Dic.colCreationTime).<(trainTime))
          || (col(Dic.colOrderEndTime).>(trainTime) && col(Dic.colCreationTime).<(trainTime)) )
      )
    val joinKeysUserId=Seq(Dic.colUserId)
    trainOrderOld=allUsersDataFrame.join(trainOrderOld,joinKeysUserId,"inner")
    //trainOrderOld.show()
    val trainOld=trainOrderOld.select(col(Dic.colUserId)).distinct()
    //
    var trainOldDataFrame=trainOrderOld.select(col(Dic.colUserId)).distinct()
    println("老用户的数量："+trainOldDataFrame.count())
    val trainPosWithLabel=trainPos.select(col(Dic.colUserId)).distinct().withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))
    println("正样本用户的数量："+trainPosWithLabel.count())
    trainOldDataFrame=trainOldDataFrame.join(trainPosWithLabel,joinKeysUserId,"left")
    trainOldDataFrame=trainOldDataFrame.na.fill(0)
    //trainOldDataFrame.show()

    var trainOldPos=trainOldDataFrame.filter(col(Dic.colOrderStatus).===(1))
    var trainOldNeg=trainOldDataFrame.filter(col(Dic.colOrderStatus).===(0))

    println("老用户正样本数量："+trainOldPos.count())
    println("老用户负样本数量："+trainOldNeg.count())
    if(trainOldNeg.count()>trainOldPos.count()*6) {
      trainOldNeg=trainOldNeg.sample(1.0).limit((trainOldPos.count()*6).toInt)
    }
    val trainOldResult=trainOldPos.union(trainOldNeg)

    //trainOldResult.show()
    println("老用户数据集生成完成！")
    trainOldResult.write.mode(SaveMode.Overwrite).format("parquet").save(oldUserSavePath+"trainusersold"+args(0))






    //构造新用户的训练样本，首先找出新用户
    //order中在train_time时间段支付套餐订单且不是老用户的用户为新用户的正样本，其余非老用户为负样本
    var trainPosUsers=orderPackage
      .filter(
        col(Dic.colCreationTime).>=(trainTime)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).>(1)
      ).select(col(Dic.colUserId)).distinct()
    trainPosUsers=trainPosUsers.except(trainOld)
   // trainPosUsers.show()
    //println(trainPosUsers.count())
    val trainNewPos=trainPosUsers.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId)))

    var trainNegOrderUsers=orderPackage
      .filter(
        col(Dic.colCreationTime).>=(trainTime)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).<=(1)
      ).select(col(Dic.colUserId)).distinct()
    trainNegOrderUsers=trainNegOrderUsers.except(trainOld).except(trainPosUsers)

    var trainPlay= play
          .filter(
            col(Dic.colPlayEndTime).===(trainTime)
            && col(Dic.colBroadcastTime)>120
          ).select(col(Dic.colUserId)).distinct()
    trainPlay=trainPlay.except(trainOld).except(trainPosUsers).except(trainNegOrderUsers)
    if(trainPlay.count()>(9*trainPosUsers.count()-trainNegOrderUsers.count())){
            trainPlay=trainPlay.sample(1).limit((9*trainPosUsers.count()-trainNegOrderUsers.count()).toInt)
          }
    val trainNewNeg=trainPlay.union(trainNegOrderUsers).withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)

    val trainNewResult=trainNewPos.union(trainNewNeg)
    println("新用户正样本数量："+trainNewPos.count())
    println("新用户负样本数量："+trainNewNeg.count())


    println("新用户数据集生成完成！")
    trainNewResult.write.mode(SaveMode.Overwrite).format("parquet").save(newUserSavePath+"trainusersnew"+args(0))



  }

}
