package train.common

import mam.Dic
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ClickProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("ClicksProcess")
      .master("local[6]")
      .getOrCreate()
    //"subscriberid","devicemsg","featurecode","bigversion","province","city",
    // "citylevel","areaid","time","itemtype","itemid","partitiondate"
    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colDeviceMsg, StringType),
        StructField(Dic.colFeatureCode, StringType),
        StructField(Dic.colBigVersion, StringType),
        StructField(Dic.colProvince, StringType),
        StructField(Dic.colCity, StringType),
        StructField(Dic.colCityLevel, StringType),
        StructField(Dic.colAreaId, StringType),
        StructField(Dic.colTime, StringType),
        StructField(Dic.colItemType, StringType),
        StructField(Dic.colItemId, StringType),
        StructField(Dic.colPartitionDate, StringType)

      )
    )
    //hdfs:///pay_predict/
    import org.apache.spark.sql.functions._
    //val hdfsPath="hdfs:///pay_predict/"
    val hdfsPath = ""
    val clickRawPath = hdfsPath + "data/train/common/raw/clicks/click*.txt"
    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders"
    var df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(clickRawPath)
    df = df.select(
      when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
      when(col(Dic.colDeviceMsg) === "NULL", null).otherwise(col(Dic.colDeviceMsg)).as(Dic.colDeviceMsg),
      when(col(Dic.colFeatureCode) === "NULL", null).otherwise(col(Dic.colFeatureCode)).as(Dic.colFeatureCode),
      when(col(Dic.colBigVersion) === "NULL", null).otherwise(col(Dic.colBigVersion)).as(Dic.colBigVersion),
      when(col(Dic.colProvince) === "NULL", null).otherwise(col(Dic.colProvince)).as(Dic.colProvince),
      when(col(Dic.colCity) === "NULL", null).otherwise(col(Dic.colCity)).as(Dic.colCity),
      when(col(Dic.colCityLevel) === "NULL", null).otherwise(col(Dic.colCityLevel)).as(Dic.colCityLevel),
      when(col(Dic.colAreaId) === "NULL", null).otherwise(col(Dic.colAreaId)).as(Dic.colAreaId)
    )
    print(df.count())
    print(df.distinct().count())
  }

}
