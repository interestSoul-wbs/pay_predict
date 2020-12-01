package train.userpay

import mam.Dic
import mam.Utils.{getData, printDf, saveProcessedData}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

object UserProfileProcess {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      //.master("local[6]")
      .appName("UserProfileProcessUserpayTrain")
      .getOrCreate()


    val time = args(0) + " " + args(1)
    println(time)


    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath=""


    //    val userListPath =  hdfsPath+"data/train/userpay/all_users"
    val trainUserPath = hdfsPath + "data/train/userpay/trainUsers" + args(0)
    //获得Train Users
    val df_trainUsers = getData(spark, trainUserPath)


    //训练集数据的保存路径
    val trainUserProfileSavePath = hdfsPath + "data/train/userpay/trainUserProfile" + args(0)

    /**
     * 获得用户画像并合并
     */
    val userProfilePlayPartPath = hdfsPath + "data/train/common/processed/userpay/userprofileplaypart" + args(0)
    val userProfilePreferencePartPath = hdfsPath + "data/train/common/processed/userpay/userprofilepreferencepart" + args(0)
    val userProfileOrderPartPath = hdfsPath + "data/train/common/processed/userpay/userprofileorderpart" + args(0)

    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    //合并用户画像
    val joinKeysUserId = Seq(Dic.colUserId)
    val temp = userProfilePlayPart.join(userProfilePreferencePart, joinKeysUserId, "left")
    val userProfiles = temp.join(userProfileOrderPart, joinKeysUserId, "left")

    //与所有用户进行join 获得训练集
    val df_trainSet = df_trainUsers.join(userProfiles, joinKeysUserId, "left")
    printDf("df_trainSet", df_trainSet)

    /**
     * 获得非数值型列   StringType是啥意思来着？？？
     */
    val colList = df_trainSet.columns.toList
    val colTypeList = df_trainSet.dtypes.toList
    val mapColList = ArrayBuffer[String]()
    for (elem <- colTypeList) {
      if (!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")) {
        mapColList.append(elem._1)
      }
    }

    val numColList = colList.diff(mapColList)

    val df_tempTrainSet = df_trainSet.na.fill(-1, List(Dic.colDaysSinceLastPurchasePackage, Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive, Dic.colDaysSinceFirstActiveInTimewindow))

    val df_trainSetNotNull = df_tempTrainSet.na.fill(0, numColList)

    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt"
    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()


    // 一级分类
    var videoFirstCategory = spark.read.format("csv").load(videoFirstCategoryTempPath)
    var conList = videoFirstCategory.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoFirstCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }
    //二级分类
    var videoSecondCategory = spark.read.format("csv").load(videoSecondCategoryTempPath)
    conList = videoSecondCategory.collect()
    for (elem <- conList) {
      var s = elem.toString()
      videoSecondCategoryMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }
    //标签
    var label = spark.read.format("csv").load(labelTempPath)
    conList = label.collect()
    for (elem <- conList) {
      var s = elem.toString()
      labelMap += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)

    }


    val pre = List(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference,
      Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference, Dic.colInPackageVideoTwoLevelPreference)

    var df_tempDataFrame = df_trainSetNotNull

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

    for (elem <- pre) {
      df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreference(col(elem), lit(1)))
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

    for (elem <- pre) {
      if (elem.contains(Dic.colVideoOneLevelPreference)) {
        df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoFirstCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoFirstCategoryMap.mkString(","))))
      } else {
        df_tempDataFrame = df_tempDataFrame.withColumn(elem + "_1", udfFillPreferenceIndex(col(elem + "_1"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_2", udfFillPreferenceIndex(col(elem + "_2"), lit(videoSecondCategoryMap.mkString(","))))
          .withColumn(elem + "_3", udfFillPreferenceIndex(col(elem + "_3"), lit(videoSecondCategoryMap.mkString(","))))
      }
    }
    //tempDataFrame.filter(!isnull(col("video_one_level_preference_1"))).show()
    for (elem <- pre) {
      if (elem.equals(Dic.colVideoOneLevelPreference)) {
        df_tempDataFrame = df_tempDataFrame.na.fill(videoFirstCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      } else {
        df_tempDataFrame = df_tempDataFrame.na.fill(videoSecondCategoryMap.size, List(elem + "_1", elem + "_2", elem + "_3"))
      }
    }

    //tempDataFrame.filter(col("video_one_level_preference_1")=!=13).show()
    val columnTypeList = df_tempDataFrame.dtypes.toList
    val columnList = ArrayBuffer[String]()
    for (elem <- columnTypeList) {
      if (elem._2.equals("StringType") || elem._2.equals("IntegerType")
        || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        columnList.append(elem._1)
      }
    }

    val result = df_tempDataFrame.select(columnList.map(df_tempDataFrame.col(_)): _*)

    saveProcessedData(result, trainUserProfileSavePath)
    println("Done！")

  }

}
