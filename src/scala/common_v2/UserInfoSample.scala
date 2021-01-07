package common_v2

import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import rs.common.DateTimeTool._
import rs.common.SparkSessionInit
import rs.common.SparkSessionInit.spark

/**
  * Konverse - 2020-11-30.
  * Get user infos from vodrs.t_vod_user_appversion_info.
  * SELECT sub_id, vod_version, partitiondate, license, get shunt_subid through hash sub_id.
  */
object UserInfoSample {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var partitiondateRealToday: String = _
  var realSysDate: String = _
  var realSysDateOneDayAgo: String = _

  def main(args: Array[String]): Unit = {

    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // 2020-12-1 - union1.x
    realSysDate = getRealSysDateTimeString
    realSysDateOneDayAgo = getDaysAgoAfter(realSysDate, -1)

    // 1 - Get sample-0.01 sub_id from t_vod_user_appversion_info
    val df_result = getUserInfoSample(partitiondate, license, vodVersion)
      .withColumn(Dic.colPartitiondate, lit(partitiondate))
      .withColumn(Dic.colLicense, lit(license))
      .withColumn(Dic.colVodVersion, lit(vodVersion))
      .withColumn(Dic.colSector, lit(666))

    printDf("df_result", df_result)

    //    saveResultTestDynamic(df_result)
    // 2 - save
    saveSubIdAndShuntIdInfo(df_result)
  }

  def saveResultTestDynamic(df_result: DataFrame) = {


    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        | vodrs.paypredict_user_subid_all_test_test
        |    (
        |     subscriberid string,
        |     customerid string,
        |     deviceid string,
        |     shunt_subid int
        |     )
        |PARTITIONED BY
        |    (
        |     partitiondate string,
        |     license string,
        |     vod_version string,
        |     sector int
        |     )
      """.stripMargin)

    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_subid_all_test_test
         |PARTITION
         |    (partitiondate,license,vod_version,sector)
         |SELECT
         |    subscriberid,
         |    customerid,
         |    deviceid,
         |    shunt_subid,
         |    partitiondate,
         |    license,
         |    vod_version,
         |    sector
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)

    println(insert_sql)
  }
}
