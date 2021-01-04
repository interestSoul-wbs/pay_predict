package train.common

import mam.{Dic, SparkSessionInit}
import mam.Utils._
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import mam.GetSaveData._
import mam.SparkSessionInit.spark

import scala.collection.mutable

object MediasProcess {

  def main(args: Array[String]): Unit = {

    // 1 Spark初始化
    SparkSessionInit.init()

    // 2 數據讀取
    val df_raw_medias = getRawMediaData(spark)

    printDf("输入 df_raw_medias", df_raw_medias)

    // 3 对数据进行处理 及 存儲
    // 3-1
    val df_medias_processed = mediasProcess(df_raw_medias)

    saveProcessedMedia(df_medias_processed)

    printDf("输出 df_medias_processed", df_medias_processed)

    // 3-2
    val df_label_one = getSingleStrColLabel(df_medias_processed, Dic.colVideoOneLevelClassification)

    saveLabel(df_label_one, "saveaPath")

    printDf("输出 df_label_one", df_label_one)

    // 3-3
    val df_label_two = getArrayStrColLabel(df_medias_processed, Dic.colVideoTwoLevelClassificationList)

    saveLabel(df_label_two, "saveaPath")

    printDf("输出 df_label_two", df_label_two)

    // 3-4
    val df_label_tags = getArrayStrColLabel(df_medias_processed, Dic.colVideoTagList)

    saveLabel(df_label_tags, "saveaPath")

    printDf("输出 df_label_tags", df_label_tags)

    println("MediasProcess over~~~~~~~~~~~")
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
        when(col(Dic.colStorageTime).isNotNull, udfLongToTimestampV2(col(Dic.colStorageTime)))
          .otherwise(null).as(Dic.colStorageTime),
        col(Dic.colVideoTime),
        col(Dic.colScore),
        col(Dic.colIsPaid),
        col(Dic.colPackageId),
        col(Dic.colIsSingle),
        col(Dic.colIsTrailers),
        col(Dic.colSupplier),
        col(Dic.colIntroduction))
      .dropDuplicates(Dic.colVideoId)
      .na.drop("all")
      //是否单点 是否付费 是否先导片 一级分类空值
      .na.fill(
      Map(
        (Dic.colIsSingle, 0),
        (Dic.colIsPaid, 0),
        (Dic.colIsTrailers, 0),
        (Dic.colVideoOneLevelClassification, "其他")
      ))
      .withColumn(Dic.colVideoTwoLevelClassificationList,
        when(col(Dic.colVideoTwoLevelClassificationList).isNotNull, col(Dic.colVideoTwoLevelClassificationList))
          .otherwise(Array("其他")))
      .withColumn(Dic.colVideoTagList,
        when(col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList))
          .otherwise(Array("其他")))

    val df_medias_mean = meanFill(df_modified_format, Array(Dic.colVideoTime, Dic.colScore))

    df_medias_mean
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

    imputer.fit(dfModifiedFormat).transform(dfModifiedFormat)

  }


  def getArrayStrColLabel(dfMedias: DataFrame, colName: String) = {

    val df_label = dfMedias
      .select(
        explode(
          col(colName)).as(colName))
      .dropDuplicates()
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).cast(StringType).as(Dic.colContent))

    df_label
  }

  def getSingleStrColLabel(df_medias: DataFrame, colName: String) = {

    val df_label = df_medias
      .select(col(colName))
      .dropDuplicates()
      .filter(!col(colName).cast("String").contains("]"))
      .withColumn(Dic.colRank, row_number().over(Window.orderBy(col(colName))) - 1)
      .select(
        concat_ws("\t", col(Dic.colRank), col(colName)).as(Dic.colContent))

    df_label
  }

}
