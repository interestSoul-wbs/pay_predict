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
    sysParamSetting
    SparkSessionInit.init()

    // 2 數據讀取
    val df_raw_medias = getRawMediaData(spark)

    printDf("输入 df_raw_medias", df_raw_medias)

    // 3 对数据进行处理 及 存儲
    // 3-1
    val df_medias_processed = mediasProcess(df_raw_medias)
    printDf("输出 df_medias_processed", df_medias_processed)

    saveProcessedMedia(df_medias_processed)

    // 3-2
    val df_label_one = getSingleStrColLabel(df_medias_processed, Dic.colVideoOneLevelClassification)

    saveLabel(df_label_one, "videofirstcategorytemp")

    printDf("输出 df_label_one", df_label_one)

    // 3-3
    val df_label_two = getArrayStrColLabel(df_medias_processed, Dic.colVideoTwoLevelClassificationList)

    saveLabel(df_label_two, "videosecondcategorytemp")

    printDf("输出 df_label_two", df_label_two)

    // 3-4
    val df_label_tags = getArrayStrColLabel(df_medias_processed, Dic.colVideoTagList)

    saveLabel(df_label_tags, "labeltemp")

    printDf("输出 df_label_tags", df_label_tags)

    println("MediasProcess over~~~~~~~~~~~")
  }

  def mediasProcess(df_raw_media: DataFrame) = {

    // Konverse -
    // 注意变量命名；
    // 避免使用 var；
    val df_modified_format = df_raw_media
      .select(
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        when(col(Dic.colVideoTitle) === "NULL", null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
        when(col(Dic.colVideoOneLevelClassification) === "NULL" or (col(Dic.colVideoOneLevelClassification) === ""), null)
          .otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
        from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
        from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
        from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
        from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
        when(col(Dic.colCountry) === "NULL", null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
        when(col(Dic.colLanguage) === "NULL", null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
        when(col(Dic.colReleaseDate) === "NULL", null).otherwise(col(Dic.colReleaseDate)).as(Dic.colReleaseDate),
        when(col(Dic.colStorageTime) === "NULL", null).otherwise(udfLongToTimestampV2(col(Dic.colStorageTime))).as(Dic.colStorageTime),
        when(col(Dic.colVideoTime) === "NULL", null).otherwise(col(Dic.colVideoTime).cast(DoubleType)).as(Dic.colVideoTime),
        when((col(Dic.colScore) === "NULL")||(col(Dic.colScore) === 0), null).otherwise(col(Dic.colScore).cast(DoubleType)).as(Dic.colScore),
        when(col(Dic.colIsPaid) === "NULL", null).otherwise(col(Dic.colIsPaid).cast(DoubleType)).as(Dic.colIsPaid),
        when(col(Dic.colPackageId) === "NULL", null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
        when(col(Dic.colIsSingle) === "NULL", null).otherwise(col(Dic.colIsSingle).cast(DoubleType)).as(Dic.colIsSingle),
        when(col(Dic.colIsTrailers) === "NULL", null).otherwise(col(Dic.colIsTrailers).cast(DoubleType)).as(Dic.colIsTrailers),
        when(col(Dic.colSupplier) === "NULL", null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
        when(col(Dic.colIntroduction) === "NULL", null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction))
      .dropDuplicates(Dic.colVideoId)
      .na.drop("all")
      //是否单点 是否付费 是否先导片 一级分类空值
      .na.fill(
      Map(
        (Dic.colPackageId, 0),
        (Dic.colIsSingle, 0),
        (Dic.colIsPaid, 0),
        (Dic.colIsTrailers, 0),
        (Dic.colVideoOneLevelClassification, "其他")
      ))
      .withColumn(Dic.colVideoTwoLevelClassificationList,
        when(
          col(Dic.colVideoTwoLevelClassificationList).isNotNull,
          col(Dic.colVideoTwoLevelClassificationList))
          .otherwise(Array("其他"))
      )
      .withColumn(Dic.colVideoTagList,
        when(
          col(Dic.colVideoTagList).isNotNull, col(Dic.colVideoTagList)
        )
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
      .withColumn(Dic.colIndex, row_number().over(Window.orderBy(col(colName))) - 1)

    df_label
  }

  def getSingleStrColLabel(df_medias: DataFrame, colName: String) = {

    val df_label = df_medias
      .select(col(colName))
      .dropDuplicates()
      .filter(!col(colName).cast("String").contains("]"))
      .withColumn(Dic.colIndex, row_number().over(Window.orderBy(col(colName))) - 1)

    df_label
  }

}
