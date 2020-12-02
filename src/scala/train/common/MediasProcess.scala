package train.common

import mam.Dic
import mam.Utils.{printDf, saveProcessedData, udfGetDays, udfLongToTimestampV2}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable

object MediasProcess {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("MediasProcess")
      .master("local[6]")
      .getOrCreate()


    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"
    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp.txt" ///pay_predict/data/train/common/processed

    // 获取原始数据
    val df_rawMedias = getRawMedias(spark)

    printDf("输入 mediasRaw", df_rawMedias)

    // 对数据进行处理
    val df_medias_processed = mediasProcess(df_rawMedias)

    saveProcessedData(df_medias_processed, mediasProcessedPath)

    printDf("df_medias_processed", df_medias_processed)

    // 保存标签
    getSingleStrColLabelAndSave(df_medias_processed, Dic.colVideoOneLevelClassification, videoFirstCategoryTempPath)
    getArrayStrColLabelAndSave(df_medias_processed, Dic.colVideoTwoLevelClassificationList, videoSecondCategoryTempPath)
    getArrayStrColLabelAndSave(df_medias_processed, Dic.colVideoTagList, labelTempPath)

    // 导演 演员尚未处理 目前还未使用到


    println("媒资数据处理完成！")

  }

  /**
    * @author wj
    * @param [spark , rawMediasPath]
    * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    * @description 读取原始文件
    */
  def getRawMedias(spark: SparkSession) = {

    val hdfsPath = ""

    val mediasRawPath = hdfsPath + "data/train/common/raw/medias/*"

    val schema = StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colVideoTitle, StringType),
        StructField(Dic.colVideoOneLevelClassification, StringType),
        StructField(Dic.colVideoTwoLevelClassificationList, StringType),
        StructField(Dic.colVideoTagList, StringType),
        StructField(Dic.colDirectorList, StringType),
        StructField(Dic.colActorList, StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField(Dic.colReleaseDate, StringType),
        StructField(Dic.colStorageTime, StringType),
        //视频时长
        StructField(Dic.colVideoTime, StringType),
        StructField(Dic.colScore, StringType),
        StructField(Dic.colIsPaid, StringType),
        StructField(Dic.colPackageId, StringType),
        StructField(Dic.colIsSingle, StringType),
        //是否片花
        StructField(Dic.colIsTrailers, StringType),
        StructField(Dic.colSupplier, StringType),
        StructField(Dic.colIntroduction, StringType)
      )
    )

    // Konverse - 注意 df 的命名 - df_相关属性 - 不要 dfRawMedia
    val df_raw_media = spark.read
      .option("delimiter", "\\t")
      .option("header", false)
      .schema(schema)
      .csv(mediasRawPath)
      .select(
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        when(col(Dic.colVideoTitle) === "NULL", null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
        when(col(Dic.colVideoOneLevelClassification) === "NULL" or (col(Dic.colVideoOneLevelClassification) === ""), null).otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
        from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
        from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
        from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
        from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
        when(col(Dic.colVideoOneLevelClassification).isNotNull, col(Dic.colVideoOneLevelClassification)), // Konverse - 这一步 相当于缺失值填充，被移动到了 process
        when(col(Dic.colCountry) === "NULL", null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
        when(col(Dic.colLanguage) === "NULL", null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
        when(col(Dic.colReleaseDate) === "NULL", null).otherwise(col(Dic.colReleaseDate)).as(Dic.colReleaseDate),
        when(col(Dic.colStorageTime) === "NULL", null).otherwise(col(Dic.colStorageTime)).as(Dic.colStorageTime), // Konverse - 这一步的udf被移动到了 process
        when(col(Dic.colVideoTime) === "NULL", null).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
        when(col(Dic.colScore) === "NULL", null).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
        when(col(Dic.colIsPaid) === "NULL", null).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
        when(col(Dic.colPackageId) === "NULL", null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
        when(col(Dic.colIsSingle) === "NULL", null).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
        when(col(Dic.colIsTrailers) === "NULL", null).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
        when(col(Dic.colSupplier) === "NULL", null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
        when(col(Dic.colIntroduction) === "NULL", null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction))

    df_raw_media
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
    //      .na.drop(Array(Dic.colVideoId))   This very very very low !!!


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
    **/
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
