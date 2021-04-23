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
      //      .enableHiveSupport()
      .master("local[6]")  //数字表示分配的核数
//      .master("spark://10.102.0.198:7077")
      .config("spark.sql.crossJoin.enabled", "true")
      .config("hive.exec.dynamici.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.driver.maxResultSize", "20g")
      .getOrCreate()

    sysParamSetting()
  }
}