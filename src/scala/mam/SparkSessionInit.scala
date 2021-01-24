package mam

import org.apache.spark.sql.SparkSession
import mam.Utils.sysParamSetting

object SparkSessionInit {
  var spark: SparkSession = _

  def init(): Unit = {

    spark = SparkSession
      .builder()
      .master( "local[6]")
      .config("spark.sql.crossJoin.enabled", "true")
//      .config("spark.executor.memory", "50g")
//      //      .config("num-executors",6)
//      //      .config("executor-cores",3)
//      .config("driver-memory", "50g")
//      .config("spark.driver.host", "101.76.209.7")
//      .config("spark.driver.port", "8000")
//      .master("local[6]")
      .getOrCreate()

    sysParamSetting()

  }



}
