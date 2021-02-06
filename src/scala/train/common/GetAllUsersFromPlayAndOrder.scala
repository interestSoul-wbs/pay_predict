package train.common

import mam.GetSaveData.{getProcessedOrder, getProcessedPlay, hdfsPath, saveProcessedData}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}

object GetAllUsersFromPlayAndOrder {


  def main(args: Array[String]): Unit = {


    // 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    // 2 數據讀取
    val df_play = getProcessedPlay(spark)
    printDf("输入 df_play", df_play)

    val df_play_id = df_play.select(Dic.colUserId).dropDuplicates()


    val df_order = getProcessedOrder(spark)
    printDf(" 输入 df_order", df_order)

    val df_order_id = df_order.select(Dic.colUserId).dropDuplicates()


    val df_all_id = df_order_id.union(df_play_id).dropDuplicates()

    saveProcessedData(df_all_id, hdfsPath + "data/train/userpay/allUsersFromPLayAndOrders")

  }


}
