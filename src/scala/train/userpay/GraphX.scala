package train.userpay

import mam.GetSaveData.{getProcessedPlay, getTrainUser}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, row_number}
import train.userpay.User2Vector.{getUsersPartTimePlay, getVideoPlayedUserList, playDays}

object GraphX {

  val commonPlayNums = 1  //测试数据比较少，先用1

  def main(args: Array[String]): Unit = {


    // 1 SparkSession init
    sysParamSetting()
    SparkSessionInit.init()


    val trainTime = args(0) + " " + args(1)
    println("trainTime: ", trainTime)

    // 2 Get Data
    val df_train_users = getTrainUser(spark, trainTime)
    printDf("输入 df_train_users", df_train_users)


    val df_plays = getProcessedPlay(spark)
    printDf("输入 df_play", df_plays)


    // 3  选一段时间的用户播放历史和视频播放用户列表
    //    val df_play_part = getUsersPartTimePlay(df_plays, df_train_users, trainTime, playDays)
    //    printDf("df_play_part", df_play_part)

    val df_play_noDup = df_plays
      .dropDuplicates(Array(Dic.colUserId, Dic.colVideoId))


    // 4 对用户和用户进行分组
    //    val win1 = Window.partitionBy(Dic.colUserId).orderBy(Dic.colVideoId)

    val df_user_video = df_play_noDup
      //选择用户看过的视频，不计数
      .groupBy(Dic.colUserId, Dic.colVideoId).count()
      .drop(Dic.colCount)
      .withColumnRenamed(Dic.colUserId, Dic.colUserIdLeft)


    printDf("df_user_video", df_user_video)


    val df_video_user = df_play_noDup
      .groupBy(Dic.colVideoId, Dic.colUserId).count()
      .drop(Dic.colCount)
      .withColumnRenamed(Dic.colUserId, Dic.colUserIdRight)

    printDf("df_video_user", df_video_user)


    // 节点和节点属性图
    val df_join_play = df_user_video
      .join(df_video_user, Seq(Dic.colVideoId), "inner")
      .groupBy(Dic.colUserIdLeft, Dic.colUserIdRight).count()
      .filter(
        col(Dic.colUserIdLeft) =!= col(Dic.colUserIdRight)
          && col(Dic.colCount) >= commonPlayNums
      )

    printDf("df_join_play", df_join_play)


    // 建立图





  }


}
