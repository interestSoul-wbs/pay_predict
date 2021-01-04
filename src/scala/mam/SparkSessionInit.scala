package mam

import mam.Utils.sysParamSetting
import org.apache.spark.sql.SparkSession

/**
  * Created by Konverse 2020-12-15.
  */
object SparkSessionInit {

  var spark: SparkSession = _

  def init(): Unit = {

    spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.crossJoin.enabled", "true")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    sysParamSetting()
  }
}
