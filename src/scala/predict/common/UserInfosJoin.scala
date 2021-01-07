package predict.common

import com.github.nscala_time.time.Imports.{DateTimeFormat, _}
import mam.Dic
import mam.GetSaveData._
import mam.Utils._
import org.apache.ivy.core.module.descriptor.License
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, _}
import rs.common._
import rs.common.DateTimeTool._
import rs.common.SparkSessionInit.spark

object UserInfosJoin {

  var tempTable = "temp_table"
  var partitiondate: String = _
  var license: String = _
  var vodVersion: String = _
  var date: DateTime = _
  var realSysDate: String = _
  var realSysDateOneDayAgo: String = _

  def main(args: Array[String]): Unit = {

    SparkSessionInit.init()

    partitiondate = args(0)
    license = args(1)
    vodVersion = args(2) // union1.x
    realSysDate = getRealSysDateTimeString
    realSysDateOneDayAgo = getDaysAgoAfter(realSysDate, -1)

    // 1 - prob and subscriberid
    val df_subid_prob = getAllUserProb(partitiondate, license, vodVersion)

    printDf("df_subid_prob", df_subid_prob)

    // 2 - Get subid, cusid, deviceid
    val df_user_all_infos = getOrignalAllUserInfo(realSysDateOneDayAgo)

    printDf("df_user_all_infos", df_user_all_infos)

    // 3 - join
    val df_result = df_subid_prob
      .join(df_user_all_infos, Seq(Dic.colSubscriberid))

    printDf("df_result", df_result)

    // 4 - save
    saveAllUserInfosAndProb(df_result, partitiondate, license, vodVersion)
  }

  def getAllUserProb(partitiondate: String, license: String, vod_version: String) = {

    val get_result_sql =
      s"""
         |SELECT
         |    subscriberid
         |    , prob
         |FROM
         |    vodrs.paypredict_userpay_result
         |WHERE
         |    partitiondate='$partitiondate'
         |    and license='$license'
         |    and vod_version='$vod_version'
       """.stripMargin

    val df_result = spark.sql(get_result_sql)
      .dropDuplicates()

    df_result
  }

  def saveAllUserInfosAndProb(df_result: DataFrame, partitiondate: String, license: String, vod_version: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |   vodrs.paypredict_userpay_all_result
        |    (
        |     subscriberid String
        |     ,customerid String
        |     ,deviceid String
        |     ,prob Double
        |     )
        |PARTITIONED BY
        |    (
        |    partitiondate String
        |    ,license String
        |    ,vod_version String
        |    )
      """.stripMargin)

    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_userpay_all_result
         |PARTITION
         |    (
         |     partitiondate='$partitiondate'
         |     ,license='$license'
         |     ,vod_version='$vod_version'
         |     )
         |SELECT
         |    subscriberid
         |    ,customerid
         |    ,deviceid
         |    ,prob
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)

    println(insert_sql)
  }
}
