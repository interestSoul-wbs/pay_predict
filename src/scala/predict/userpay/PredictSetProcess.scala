package predict.userpay

import mam.{Dic, SparkSessionInit}
import mam.GetSaveData.{getPredictUser, getProcessedUserMeta, getUserProfileOrderPart, getUserProfilePlayPart, getUserProfilePreferencePart, getVideoFirstCategory, getVideoLabel, getVideoSecondCategory, saveDataSet}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.collection.mutable.ArrayBuffer

object PredictSetProcess {

  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    val predictTime = args(0) + " " + args(1)
    println("predictTime", predictTime)

    // 2 Get Data

    val df_user_profile_play = getUserProfilePlayPart(spark, predictTime, "predict")
    printDf("输入 df_user_profile_play", df_user_profile_play)

    val df_user_profile_pref = getUserProfilePreferencePart(spark, predictTime, "predict")
    printDf("输入 df_user_profile_pref", df_user_profile_pref)

    val df_user_profile_order = getUserProfileOrderPart(spark, predictTime, "predict")
    printDf("输入 df_user_profile_order", df_user_profile_order)

    val df_video_first_category = getVideoFirstCategory()
    printDf("输入 df_video_first_category", df_video_first_category)

    val df_video_second_category = getVideoSecondCategory()
    printDf("输入 df_video_second_category", df_video_second_category)

    val df_label = getVideoLabel()
    printDf("输入 df_label", df_label)

    val df_predict_users = getPredictUser(spark, predictTime)
    printDf("df_predict_users", df_predict_users)


    // Click data
    val df_click_meta = getProcessedUserMeta()
    printDf("输入 df_click_meta", df_click_meta)

    // 3 Train Set Process
    val df_predict_set = predictSetProcess(df_user_profile_play, df_user_profile_pref, df_user_profile_order, df_video_first_category, df_video_second_category, df_label, df_predict_users,df_click_meta)

    // 4 Save Train Users
    saveDataSet(predictTime, df_predict_set, "predict")
    printDf("输出 df_predict_set", df_predict_set)
    println("Predict Set Process Done！")

  }
    def predictSetProcess(df_user_profile_play: DataFrame, df_user_profile_pref: DataFrame, df_user_profile_order: DataFrame,
                          df_video_first_category: DataFrame, df_video_second_category: DataFrame, df_label: DataFrame,
                          df_predict_users: DataFrame,df_click_meta:DataFrame): DataFrame = {


      // Get All User Profile Data
      val joinKeysUserId = Seq(Dic.colUserId)
      val df_play_join_pref = df_user_profile_play.join(df_user_profile_pref, joinKeysUserId, "left")
      val df_user_profile = df_play_join_pref.join(df_user_profile_order, joinKeysUserId, "left")
      val df_predict_profile = df_predict_users.join(df_user_profile, joinKeysUserId, "left")

      // Fill Null Values
      val colList = df_predict_profile.columns.toList
      val colTypeList = df_predict_profile.dtypes.toList
      val mapColList = ArrayBuffer[String]()

      for (elem <- colTypeList) {
        if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
          && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
          mapColList.append(elem._1)
        }
      }
      val numColList = colList.diff(mapColList)
      val df_tempPredictSet = df_predict_profile.na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
        Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))
      val df_predict_set_not_null = df_tempPredictSet.na.fill(0, numColList)

      /**
       * Split Pref Data
       */
      var videoFirstCategoryMap: Map[String, Int] = Map()
      var videoSecondCategoryMap: Map[String, Int] = Map()
      var labelMap: Map[String, Int] = Map()


      var conList = df_video_first_category.collect()
      for (elem <- conList) {
        var s = elem.toString()
        videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

      }

      conList = df_video_second_category.collect()
      for (elem <- conList) {
        var s = elem.toString()
        videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

      }

      conList = df_label.collect()
      for (elem <- conList) {
        var s = elem.toString()
        labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

      }


      val prefColumns = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
        Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

      var df_temp_df1 = df_predict_set_not_null

      def udfFillPreference = udf(fillPreference _)

      def fillPreference(prefer: Map[String, Int], offset: Int) = {
        if (prefer == null) {
          null
        } else {
          val mapArray = prefer.toArray
          if (mapArray.length > offset - 1) {
            mapArray(offset - 1)._1
          } else {
            null
          }

        }

      }

      for (elem <- prefColumns) {
        df_temp_df1 = df_temp_df1.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
          .withColumn(elem + "_2", udfFillPreference(col(elem), lit(2)))
          .withColumn(elem + "_3", udfFillPreference(col(elem), lit(3)))
      }

      def udfFillPreferenceIndex = udf(fillPreferenceIndex _)

      def fillPreferenceIndex(prefer: String, mapLine: String) = {
        if (prefer == null) {
          null
        } else {
          var tempMap: Map[String, Int] = Map()
          var lineIterator1 = mapLine.split(",")
          //迭代打印所有行
          lineIterator1.foreach(m => tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
          tempMap.get(prefer)
        }
      }

      var df_temp_df2 = df_temp_df1

      for (elem <- prefColumns) {
        if (elem.contains(Dic.colVideoOneLevelPreference)) {
          df_temp_df2 = df_temp_df2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
            .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
            .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
        } else {
          df_temp_df2 = df_temp_df2.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
            .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
            .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
        }
      }

      var df_temp_df3 = df_temp_df2
      for (elem <- prefColumns) {
        if (elem.equals(Dic.colVideoOneLevelPreference)) {
          df_temp_df3 = df_temp_df3.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
        } else {
          df_temp_df3 = df_temp_df3.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
        }

      }
      //df_tempDataFrame.filter(col("video_one_level_preference_1")=!=13).show()
      val columnTypeList = df_temp_df3.dtypes.toList
      val columnList = ArrayBuffer[String]()
      for (elem <- columnTypeList) {
        if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
          || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
          columnList.append(elem._1)
        }
      }

      val df_predict_user_profile = df_temp_df3.select(columnList.map(df_temp_df3.col(_)): _*)



      val df_train_click = df_predict_user_profile.join(df_click_meta, joinKeysUserId, "left")
        .na.fill(-1)


      /**
       * 将添加用户的标签信息
       */

      df_train_click
//      df_predict_user_profile
    }
}