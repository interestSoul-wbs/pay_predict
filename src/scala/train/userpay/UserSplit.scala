package train.userpay

import mam.GetSaveData._
import mam.Dic
import mam.Utils._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, isnull, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.github.nscala_time.time.Imports._

object UserSplit {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var timeWindow: Int = 30
  var date: DateTime = _
  var thirtyDaysAgo: String = _
  
  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    date = DateTime.parse(partitiondate, DateTimeFormat.forPattern("yyyyMMdd"))
    thirtyDaysAgo = (date - 30.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))
    
    // 1 - SparkSession and params initialize
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 训练集的划分时间点 - 输入时间的30天之前
    println("thirtyDaysAgo is : " + thirtyDaysAgo)

    // 2 - processed df_plays
    val df_plays = getProcessedPlay(spark, partitiondate, license)

    //所有用户id的dataframe
    val df_all_users = df_plays.select(col(Dic.colUserId)).distinct()

    val df_orders = getProcessedOrder(spark, partitiondate, license)

    // 选择套餐订单
    val df_order_package = df_orders
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4))

    //order中在train_time后14天内的支付成功订单
    val trainTimePost14 = calDate(thirtyDaysAgo, days = 14)

    val df_train_pos = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && col(Dic.colCreationTime).>=(thirtyDaysAgo)
          && col(Dic.colCreationTime).<(trainTimePost14))

    //println("df_train_pos.shape："+df_train_pos.count())
    //在time-time_window到time时间段内成功支付过订单 或者 在time之前创建的订单到time时仍旧有效
    val trainTimePre = calDate(thirtyDaysAgo, days = -timeWindow)
    var df_train_order_old = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && ((col(Dic.colCreationTime).>(trainTimePre) && col(Dic.colCreationTime).<(thirtyDaysAgo))
          || (col(Dic.colOrderEndTime).>(thirtyDaysAgo) && col(Dic.colCreationTime).<(thirtyDaysAgo))))

    val joinKeysUserId = Seq(Dic.colUserId)
    df_train_order_old = df_all_users.join(df_train_order_old, joinKeysUserId, "inner")

    val df_train_old = df_train_order_old.select(col(Dic.colUserId)).distinct()

    var trainOldDataFrame = df_train_order_old.select(col(Dic.colUserId)).distinct()
    println("老用户的数量：" + trainOldDataFrame.count())

    val trainPosWithLabel = df_train_pos
      .select(col(Dic.colUserId))
      .distinct()
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)))
    println("正样本用户的数量：" + trainPosWithLabel.count())

    trainOldDataFrame = trainOldDataFrame.join(trainPosWithLabel, joinKeysUserId, "left")
    trainOldDataFrame = trainOldDataFrame.na.fill(0)

    var trainOldPos = trainOldDataFrame.filter(col(Dic.colOrderStatus).===(1))

    var trainOldNeg = trainOldDataFrame.filter(col(Dic.colOrderStatus).===(0))

    println("老用户正样本数量：" + trainOldPos.count())
    println("老用户负样本数量：" + trainOldNeg.count())

    if (trainOldNeg.count() > trainOldPos.count() * 6) {
      trainOldNeg = trainOldNeg.sample(1.0).limit((trainOldPos.count() * 6).toInt)
    }
    val df_train_old_result = trainOldPos.union(trainOldNeg)

    printDf("df_train_old_result", df_train_old_result)

    println("老用户数据集生成完成！")

    saveUserSplitResult(spark, df_train_old_result, partitiondate, license, "train", "old")

    //构造新用户的训练样本，首先找出新用户
    //order中在train_time时间段支付套餐订单且不是老用户的用户为新用户的正样本，其余非老用户为负样本
    var trainPosUsers = df_order_package
      .filter(
        col(Dic.colCreationTime).>=(thirtyDaysAgo)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).>(1))
      .select(col(Dic.colUserId)).distinct()

    trainPosUsers = trainPosUsers.except(df_train_old)

    val trainNewPos = trainPosUsers.withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)))

    var trainNegOrderUsers = df_order_package
      .filter(
        col(Dic.colCreationTime).>=(thirtyDaysAgo)
          && col(Dic.colCreationTime).<(trainTimePost14)
          && col(Dic.colOrderStatus).<=(1))
      .select(col(Dic.colUserId))
      .distinct()

    trainNegOrderUsers = trainNegOrderUsers.except(df_train_old).except(trainPosUsers)

    var trainPlay = df_plays
      .filter(
        col(Dic.colPlayEndTime).===(thirtyDaysAgo)
          && col(Dic.colBroadcastTime) > 120)
      .select(col(Dic.colUserId))
      .distinct()

    trainPlay = trainPlay.except(df_train_old).except(trainPosUsers).except(trainNegOrderUsers)

    if (trainPlay.count() > (9 * trainPosUsers.count() - trainNegOrderUsers.count())) {
      trainPlay = trainPlay.sample(1).limit((9 * trainPosUsers.count() - trainNegOrderUsers.count()).toInt)
    }

    val trainNewNeg = trainPlay
      .union(trainNegOrderUsers)
      .withColumn(Dic.colOrderStatus, udfAddOrderStatus(col(Dic.colUserId)) - 1)

    val trainNewResult = trainNewPos.union(trainNewNeg)
    println("新用户正样本数量：" + trainNewPos.count())
    println("新用户负样本数量：" + trainNewNeg.count())

    printDf("trainNewResult", trainNewResult)

    println("新用户数据集生成完成！")

    saveUserSplitResult(spark, trainNewResult, partitiondate, license, "train", "new")
  }


}
