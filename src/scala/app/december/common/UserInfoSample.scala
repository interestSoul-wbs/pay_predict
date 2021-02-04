package app.december.common

import mam.Dic
import mam.GetSaveData.{getUserInfoSample, saveSubIdAndShuntIdInfoDynamic}
import mam.Utils.printDf
import org.apache.spark.sql.functions.lit
import rs.common.DateTimeTool.{getDaysAgoAfter, getRealSysDateTimeString}
import rs.common.SparkSessionInit

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

    // 2 - save
    saveSubIdAndShuntIdInfoDynamic(df_result)
  }


}
