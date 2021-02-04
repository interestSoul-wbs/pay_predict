package app.december.common

import mam.Dic
import mam.GetSaveData.{getOrignalAllUserInfo, getOrignalSubId, saveSubIdAndShuntIdInfoDynamic}
import mam.Utils.udfUidHash
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType
import rs.common.DateTimeTool.{getDaysAgoAfter, getRealSysDateTimeString}
import rs.common.SparkSessionInit

/**
  * Konverse - 2020-11-30.
  * Get user infos from vodrs.t_vod_user_appversion_info.
  * SELECT sub_id, vod_version, partitiondate, license, get shunt_subid through hash sub_id.
  */
object UserInfo {

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

    // 1 - Get original sub_id from t_vod_user_appversion_info
    val df_sub_id = getOrignalSubId(realSysDateOneDayAgo, license, vodVersion)

    // 2 - Get subid, cusid, deviceid
    val df_user_all_infos = getOrignalAllUserInfo(realSysDateOneDayAgo)

    // 3 - join
    val df_result = df_sub_id
      .join(df_user_all_infos, Seq(Dic.colSubscriberid))
      .select(
        col(Dic.colSubscriberid).cast(StringType),
        col(Dic.colCustomerid).cast(StringType),
        col(Dic.colDeviceid).cast(StringType))
      .dropDuplicates(Array(Dic.colSubscriberid))
      .withColumn(Dic.colShuntSubid, udfUidHash(col(Dic.colSubscriberid)) % 100)
      .withColumn(Dic.colSector,col(Dic.colShuntSubid) % 4)
      .withColumn(Dic.colPartitiondate, lit(partitiondate))
      .withColumn(Dic.colLicense, lit(license))
      .withColumn(Dic.colVodVersion, lit(vodVersion))

    // 4 - save
    saveSubIdAndShuntIdInfoDynamic(df_result)
  }
}
