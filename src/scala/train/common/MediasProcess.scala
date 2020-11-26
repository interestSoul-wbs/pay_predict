package train.common
/**
 * @Author wj
 * @Date 2020/09
 * @Version 1.0
 */
import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat

import mam.Dic
import mam.GetSaveData.getRawMedias
import mam.Utils.{printArray, printDf, udfLongToTimestamp, udfLongToTimestampV2}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.expressions.Window

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Set}
import org.apache.spark.sql.functions._

object MediasProcess {

  //将字符串属性转化为

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("MediasProcess")
      //.master("local[6]")
      .getOrCreate()


    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val mediasRawPath=hdfsPath+"data/train/common/raw/medias/*"
    val mediasProcessedPath=hdfsPath+"data/train/common/processed/mediastemp"
    val videoFirstCategoryTempPath=hdfsPath+"data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath=hdfsPath+"data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath=hdfsPath+"data/train/common/processed/labeltemp.txt"///pay_predict/data/train/common/processed
    //1.获取原始数据
    var dfRawMedias=getRawMedias(spark,mediasRawPath)

    printDf("输入 mediasRaw",dfRawMedias)
    //2、对数据进行处理
    var dfModifiedFormat=mediasProcess(dfRawMedias)

    //3、对特定的列进行均值填充
    val cols = Array(Dic.colScore, Dic.colVideoTime)
    var dfMeanFill=meanFill(dfModifiedFormat,cols)

    //4、保存标签
    getSingleStrColLabelAndSave(dfMeanFill,Dic.colVideoOneLevelClassification,videoFirstCategoryTempPath)
    getArrayStrColLabelAndSave(dfMeanFill,Dic.colVideoTwoLevelClassificationList,videoSecondCategoryTempPath)
    getArrayStrColLabelAndSave(dfMeanFill,Dic.colVideoTagList,labelTempPath)

    //5、保存处理好的数据
    saveProcessedData(dfMeanFill,mediasProcessedPath)
    printDf("输出  mediasProcessed",dfMeanFill)
     println("媒资数据处理完成！")



  }




  def mediasProcess(rawMediasData:DataFrame)={
    /**
    *@author wj
    *@param [rawMediasData]
    *@return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    *@description  更改数据格式，主要将NULL字符串转化为null或者NaN
    */
    var dfModifiedFormat=rawMediasData.select(
      when(col(Dic.colVideoId)==="NULL",null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
      when(col(Dic.colVideoTitle)==="NULL",null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
      when(col(Dic.colVideoOneLevelClassification)==="NULL",null).otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
      from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
      from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
      from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
      from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
      when(col(Dic.colCountry)==="NULL",null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
      when(col(Dic.colLanguage)==="NULL",null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
      when(col(Dic.colReleaseDate)==="NULL",null).otherwise(col(Dic.colReleaseDate) ).as(Dic.colReleaseDate),
      when(col(Dic.colStorageTime)==="NULL",null).otherwise(udfLongToTimestampV2(col(Dic.colStorageTime ))).as(Dic.colStorageTime),
      when(col(Dic.colVideoTime)==="NULL",null).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
      when(col(Dic.colScore)==="NULL",null).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
      when(col(Dic.colIsPaid)==="NULL",null).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
      when(col(Dic.colPackageId)==="NULL",null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
      when(col(Dic.colIsSingle)==="NULL",null).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
      when(col(Dic.colIsTrailers)==="NULL",null).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
      when(col(Dic.colSupplier)==="NULL",null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
      when(col(Dic.colIntroduction)==="NULL",null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction)
    )
    dfModifiedFormat.dropDuplicates().na.drop("all")

  }

  def meanFill(dfModifiedFormat:DataFrame, cols: Array[String])={
    /**
    *@author wj
    *@param [dfModifiedFormat]
    *@return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
    *@description  将指定的列使用均值进行填充
    */
    val imputer = new Imputer()
      .setInputCols(cols)
      .setOutputCols(cols)
      .setStrategy("mean")

    val dfMeanFill=imputer.fit(dfModifiedFormat).transform(dfModifiedFormat)
    dfMeanFill
  }
  def getArrayStrColLabelAndSave(dfMedias: DataFrame, colName: String, labelSavedPath:String) = {

    val df_label = dfMedias
      .select(
        explode(
          col(colName)).as(colName))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).cast(StringType).as(Dic.colContent))

    printDf("输出  df_label", df_label)

    saveLabel(df_label,labelSavedPath)
  }

  def getSingleStrColLabelAndSave(dfMedias: DataFrame, colName: String,labelSavedPath:String) = {

    val dfLabel = dfMedias
      .select(col(colName))
      .dropDuplicates()
      .filter(!col(colName).cast("String").contains("]"))
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).as(Dic.colContent))

    printDf("输出  df_label", dfLabel)

    saveLabel(dfLabel,labelSavedPath)
  }

  def saveLabel(dfLabel: DataFrame,labelSavedPath:String) = {

    dfLabel.coalesce(1).write.mode(SaveMode.Overwrite).option("header","false").csv(labelSavedPath)
  }

  def saveProcessedData(dfMediasProcessed: DataFrame,mediasProcessedPath:String) = {

    dfMediasProcessed.write.mode(SaveMode.Overwrite).format("parquet").save(mediasProcessedPath)
  }
}
