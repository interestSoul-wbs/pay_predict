package predict.common

import mam.GetSaveData._
import mam.SparkSessionInit.spark
import mam.Utils.{calDate, printDf, sysParamSetting, udfSortByPlayTime}
import mam.{Dic, SparkSessionInit}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * @author wj
 * @date 2020/11/6
 * @version 0.1
 * @describe   收集最近一段时间用户的播放历史和全部的单点订单的历史
 */
object UserOrderAndPlayHistory {
  def main(args: Array[String]): Unit = {
    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    // 2 Get Data
    val now = args(0) + " " + args(1)

    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_plays", df_plays)

    val df_medias = getProcessedMedias(spark)
    printDf("输入 df_medias", df_medias)

    val df_orders=getProcessedOrder(spark)
    printDf("输入 df_orders", df_orders)



    val df_order_list=getUserOrdersList(df_orders,now)
    val df_play_list=getUserPlaysList(df_plays,now,df_medias)
    printDf("输出 orderList",df_order_list)
    printDf("输出 playList",df_play_list)

    //3 Save Data
    savePlayList(now,df_play_list,"predict")
    saveOrderList(now,df_order_list,"predict")

    println("UserOrderAndPlayHistory over~~~~~~~~~~~")






  }
  def getUserPlaysList(plays:DataFrame,now:String,medias:DataFrame)={
    //选取最近一周的观看历史
   // print(calDate(now,-7))
   val joinKeysVideoId=Seq(Dic.colVideoId)
    var playsList=plays.join(medias.select(col(Dic.colVideoId),
      col(Dic.colVideoOneLevelClassification),col(Dic.colIsPaid)),joinKeysVideoId,"inner")
    val playsSelect=playsList.filter(
      col(Dic.colPlayEndTime).<(now)
      && col(Dic.colPlayEndTime).>(calDate(now,-7))
        && col(Dic.colBroadcastTime).>(360)
        && (col(Dic.colIsPaid).===(1) || col(Dic.colVideoOneLevelClassification).===("电影"))
    ).groupBy(col(Dic.colUserId))
      .agg(udfSortByPlayTime(collect_list(struct(col(Dic.colVideoId),col(Dic.colPlayEndTime)))).as(Dic.colPlayList))
      .select(col(Dic.colUserId),col(Dic.colPlayList))

    //平均每个用户每周会看个视频
    playsSelect
  }
  def getUserOrdersList(orders:DataFrame,now:String)={

    val orderSinglePoint=orders.filter(
      col(Dic.colOrderStartTime).<(now)
     // && col(Dic.colOrderStatus).>(1)  是否要考虑未支付订单的作用
      && col(Dic.colResourceType).===(0)
    )
  //orderSinglePoint.withColumn("row_number",row_number().over(Window.partitionBy(Dic.colUserId).orderBy(col(Dic.colCreationTime).desc))).show()
    orderSinglePoint
      .select(col(Dic.colUserId),col(Dic.colResourceId),col(Dic.colCreationTime).cast("string"))
      .groupBy(col(Dic.colUserId))
      .agg(udfSortByPlayTime(collect_list(struct(col(Dic.colResourceId),col(Dic.colCreationTime)))).as(Dic.colOrderList))
      .select(col(Dic.colUserId),col(Dic.colOrderList))


  }


}
