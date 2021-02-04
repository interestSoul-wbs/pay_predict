package app.december.train.userpay

import com.github.nscala_time.time.Imports.DateTime
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, _}
import rs.common.SparkSessionInit
import rs.common.SparkSessionInit._

object UserSplitForTrain {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var sector: Int = _
  var category: String = _
  var processedMediasData: String = _
  var date: DateTime = _
  val timeLength = 14
  val predictResourceId = Array(100201,100202,101101,101103,101301,101601,100701,100801,101803,101810) //要预测的套餐id

  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // 2020-12-1 - union1.x
    sector = args(3).toInt // 用户分群后的群代号
    category = args(4)
    processedMediasData = args(5)

    println(partitiondate, license, vodVersion, sector)

    var df_all_user_info = spark.emptyDataFrame

    if (category.equals("train")) {

      val trainTime = partitiondateToStandard(partitiondate)
      println("trainTime", trainTime)

      // 2 Get Data
      val df_orders = getProcessedOrderV2(partitiondate, license, vodVersion, sector, category)

      //所有用户id的dataframe  Hisense data
      val df_all_users = getAllUsers(processedMediasData, license, vodVersion, sector)

      // 3 训练集正负样本选取
      df_all_user_info = getTrainSetUsers(df_all_users, df_orders, trainTime, timeLength, predictResourceId)

    } else {

      df_all_user_info = getAllUsers(processedMediasData, license, vodVersion, sector)
        .select(
          col(Dic.colUserId)
          , lit(-1).as(Dic.colOrderStatus))
    }

    // 4 Save Train Users
    saveUsersSplitDataV2(df_all_user_info, partitiondate, license, vodVersion, sector, category)
  }

  def getTrainSetUsers(df_allUsers: DataFrame, df_orders: DataFrame, trainTime: String, timeLength: Int, predictResourceId: Array[Int]): DataFrame = {

    /**
      * 训练集正负样本选取
      * 正样本：选择在order中trainTime到 未来14天 内的成功购买用户
      * 负样本：选择正样本以外的10倍用户
      */

    /**
      * 标记金额信息异常用户 money is 0 or 1 RMB
      */
    val df_order = df_orders
      .withColumn(Dic.colIsMoneyError, udfGetErrorMoneySign(col(Dic.colResourceType), col(Dic.colMoney)))

    //金额异常用户
    val df_illegal_users = df_order
      .filter(
        col(Dic.colCreationTime) >= trainTime
          && col(Dic.colCreationTime) < calDate(trainTime, timeLength)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4)
          && col(Dic.colResourceId).isin(predictResourceId: _*)
          && (col(Dic.colIsMoneyError) === 1))
      .select(Dic.colUserId)
      .distinct()

    //  正样本
    val df_train_pos_users = df_order
      .filter(
        col(Dic.colCreationTime) >= trainTime
          && col(Dic.colCreationTime) < calDate(trainTime, timeLength)
          && col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4)
          && col(Dic.colResourceId).isin(predictResourceId: _*)
          && col(Dic.colOrderStatus).>(1))
      .select(Dic.colUserId)
      .distinct()
      .except(df_illegal_users)
      .withColumn(Dic.colOrderStatus, lit(1))

    //负样本
    val df_all_neg_users = df_allUsers
      .except(df_train_pos_users.select(Dic.colUserId))
      .except(df_illegal_users) //except 保证column一致

    val df_train_neg_users = df_all_neg_users
      .sample(1)
      .limit(10 * df_train_pos_users.count().toInt)
      .withColumn(Dic.colOrderStatus, lit(0))

    df_train_pos_users.union(df_train_neg_users).distinct().sample(1)
  }
}
