package predict.userpay

import mam.Dic
import mam.Utils.{calDate, printDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object PredictUserSplit {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var timeWindow: Int = 30

  def main(args: Array[String]): Unit = {

    partitiondate = args(0)
    license = args(1)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val predict_time = "2020-07-14 00:00:00"

    // 1 - processed play data
    val df_plays = getPlay(spark)

    // 2 - 所有用户id的dataframe
    val allUsersDataFrame = df_plays.select(col(Dic.colUserId)).distinct()

    // 3 - processed order data
    val df_orders = getOrder(spark)

    printDf("df_orders", df_orders)

    // 选择套餐订单
    val df_order_package = df_orders
      .filter(
        col(Dic.colResourceType).>(0)
          && col(Dic.colResourceType).<(4))

    //order中在train_time后14天内的支付成功订单
    val predictTimePost14 = calDate(predict_time, days = 14)

    val trainPos = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && col(Dic.colCreationTime).>=(predict_time)
          && col(Dic.colCreationTime).<(predictTimePost14))

    val predictTimePre = calDate(predict_time, days = -timeWindow)

    var predictOrderOld = df_order_package
      .filter(
        col(Dic.colOrderStatus).>(1)
          && ((col(Dic.colCreationTime).>(predictTimePre) && col(Dic.colCreationTime).<(predict_time))
          || (col(Dic.colOrderEndTime).>(predict_time) && col(Dic.colCreationTime).<(predict_time))))

    val joinKeysUserId = Seq(Dic.colUserId)

    predictOrderOld = allUsersDataFrame.join(predictOrderOld, joinKeysUserId, "inner")

    val df_predict_old = predictOrderOld.select(col(Dic.colUserId)).distinct()

    saveUserListData(spark, df_predict_old, "predict", "old")

    val df_predict_new = allUsersDataFrame.except(df_predict_old)

    saveUserListData(spark, df_predict_new, "predict", "new")
  }

  /**
    * Get user play data.
    *
    * @param spark
    * @return
    */
  def getPlay(spark: SparkSession) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    vodrs.t_sdu_user_play_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }

  /**
    * Get user order data.
    *
    * @param spark
    * @return
    */
  def getOrder(spark: SparkSession) = {

    // 1 - 获取用户购买记录
    val user_order_sql =
      s"""
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    vodrs.t_sdu_user_order_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_order = spark.sql(user_order_sql)

    df_order
  }

  def saveUserListData(spark: SparkSession, df_user_list: DataFrame, data_type: String, user_type: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.t_sdu_user_list_paypredict(
        |         user_id string)
        |PARTITIONED BY
        |    (partitiondate string, data_type string, user_type string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_user_list.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_sdu_user_list_paypredict
         |PARTITION
         |    (partitiondate='$partitiondate', data_type='$data_type', user_type='$user_type', license='$license')
         |SELECT
         |    user_id
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }
}
