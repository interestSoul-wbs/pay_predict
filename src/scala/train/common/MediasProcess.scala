package train.common

import mam.Dic
import mam.Utils.{printDf, saveProcessedData, udfGetDays, udfLongToTimestampV2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import mam.GetSaveData._
import scala.collection.mutable

object MediasProcess {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils") // 上传 master 时，删除
    Logger.getLogger("org").setLevel(Level.ERROR) // 上传 master 时，删除

    val spark = SparkSession
      .builder()
      .master("local[6]") // 上传 master 时，删除
//      .enableHiveSupport()
      .getOrCreate()

    // 2 - 所有这些 文件 的读取和存储路径，全部包含到函数里，不再当参数传入，见例子 - getRawMediaData
    // 以下的这些 路径， 都不应该在存在于这个文件中， 见例子 - getRawMediaData 的读取方式
    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt"

    // 3 - 获取原始数据
    // 在 GetAndSaveData 中，创建同名的 读取或存储函数 - spark允许，同名但是传入参数不同的 函数存在；
    // 例： 有 2个getRawMediaData，一个是我调用的，一个是你调用的，它们同名，但是传参不一样；
    // 参考 Konverse分支中，相关读取、存储数据函数的命名；
    val df_raw_medias = getRawMediaData(spark)

    printDf("输入 mediasRaw", df_raw_medias)

    // 对数据进行处理
    val df_medias_processed = mediasProcess(df_raw_medias)

    saveProcessedData(df_medias_processed, mediasProcessedPath)

    printDf("df_medias_processed", df_medias_processed)

    // 保存标签
//    getSingleStrColLabelAndSave(df_medias_processed, Dic.colVideoOneLevelClassification, videoFirstCategoryTempPath)
//    getArrayStrColLabelAndSave(df_medias_processed, Dic.colVideoTwoLevelClassificationList, videoSecondCategoryTempPath)
//    getArrayStrColLabelAndSave(df_medias_processed, Dic.colVideoTagList, labelTempPath)

    // 导演 演员尚未处理 目前还未使用到
    println("媒资数据处理完成！")
  }

  def mediasProcess(df_raw_media: DataFrame) = {

    // Konverse -
    // 注意变量命名；
    // 避免使用 var；
    val df_modified_format = df_raw_media
      .select(
        col(Dic.colVideoId),
        col(Dic.colVideoTitle),
        col(Dic.colVideoOneLevelClassification),
        col(Dic.colVideoTwoLevelClassificationList),
        col(Dic.colVideoTagList),
        col(Dic.colDirectorList),
        col(Dic.colCountry),
        col(Dic.colActorList),
        col(Dic.colLanguage),
        col(Dic.colReleaseDate),
        udfLongToTimestampV2(col(Dic.colStorageTime)).as(Dic.colStorageTime),
        col(Dic.colVideoTime),
        col(Dic.colScore),
        col(Dic.colIsPaid),
        col(Dic.colPackageId),
        col(Dic.colIsSingle),
        col(Dic.colIsTrailers),
        col(Dic.colSupplier),
        col(Dic.colIntroduction))
      .dropDuplicates(Dic.colVideoId)
      .na.drop()
      //是否单点 是否付费 是否先导片 一级分类空值
      .na.fill(
      Map(
        (Dic.colIsSingle, 0),
        (Dic.colIsPaid, 0),
        (Dic.colIsTrailers, 0),
        (Dic.colVideoOneLevelClassification, "其他")))
      //添加新列 是否在套餐内
      .withColumn(Dic.colInPackage, when(col(Dic.colPackageId).>(0), 1).otherwise(0))


    printDf("基本处理后的med", df_modified_format)

    println("正在填充空值....")

    // 根据一级分类填充空值
    val dfMeanScoreFill = meanFillAccordLevelOne(df_modified_format, Dic.colScore)

    val dfMeanVideoTimeFill = meanFillAccordLevelOne(dfMeanScoreFill, Dic.colVideoTime)

    printDf("填充空值之后", dfMeanVideoTimeFill)


    dfMeanVideoTimeFill
  }

  /**
   * @author wj
   * @param [dfModifiedFormat ]
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 将指定的列使用均值进行填充
   */
  def meanFill(dfModifiedFormat: DataFrame, cols: Array[String]) = {


    val imputer = new Imputer()
      .setInputCols(cols)
      .setOutputCols(cols)
      .setStrategy("mean")

    val dfMeanFill = imputer.fit(dfModifiedFormat).transform(dfModifiedFormat)

    dfMeanFill
  }

  /**
   * @describe 根据video的视频一级分类进行相关列空值的填充
   * @author wx
   * @param [mediasDf ]
   * @param [spark ]
   * @return { @link DataFrame }
   * */
  def meanFillAccordLevelOne(df_medias: DataFrame, colName: String) = {

    val df_mean = df_medias
      .groupBy(Dic.colVideoOneLevelClassification)
      .agg(mean(col(colName)))
      .withColumnRenamed("avg(" + colName + ")", "mean" + colName + "AccordLevelOne")

    val meanValue = df_medias
      .agg(mean(colName))
      .collectAsList()
      .get(0)
      .get(0)
    println("Mean " + colName, meanValue)

    var meanMap = df_mean.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoOneLevelClassification).toString -> row.getAs("mean" + colName + "AccordLevelOne").toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    meanMap ++ Map(("meanValue", meanValue))
    println(meanMap)

    // 还没改完。。。。。。。。。。
    //    val df_mediasFilled = df_medias.withColumn(colName, fillUseMap(meanMap)(col(colName)))
    //
    //
    //
    //    val df_mediasJoinMean = df_medias.join(df_mean, Seq(Dic.colVideoOneLevelClassification), "inner")
    //    printDf("df_mediasJoinMean", df_mediasJoinMean)
    //
    //
    //    val df_meanFilled = df_mediasJoinMean.withColumn(colName, when(col(colName).>(0.0), col(colName))
    //      .otherwise(col("mean" + colName + "AccordLevelOne")))
    //      .na.fill(Map((colName, meanValue)))
    //      .drop("mean" + colName + "AccordLevelOne")
    //
    //    df_meanFilled
    df_mean
  }


  def getArrayStrColLabelAndSave(dfMedias: DataFrame, colName: String, labelSavedPath: String) = {

    val df_label = dfMedias
      .select(
        explode(
          col(colName)).as(colName))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).cast(StringType).as(Dic.colContent))

    printDf("输出  df_label", df_label)

    saveLabel(df_label, labelSavedPath)
  }

  def getSingleStrColLabelAndSave(dfMedias: DataFrame, colName: String, labelSavedPath: String) = {

    val dfLabel = dfMedias
      .select(col(colName))
      .dropDuplicates()
      .filter(!col(colName).cast("String").contains("]"))
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).as(Dic.colContent))

    printDf("输出  df_label", dfLabel)

    saveLabel(dfLabel, labelSavedPath)
  }

  def saveLabel(dfLabel: DataFrame, labelSavedPath: String) = {

    dfLabel.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "false").csv(labelSavedPath)
  }
}
