package rs.common

import org.apache.spark.sql.DataFrame
import mam.GetSaveData._

/**
  * Created by Konverse on 2020-5-11.
  * 轮播窗需要支持 观看后更新；
  * 离线 - 从 人工资源池 + 媒资包 计算出 item候选list； 优先使用 媒资包媒资，然后是 人工资源池；
  * 引擎 和 实时部分，去感知 运营配置好的内容 和 观看后更新的坑位的 内容；
  */
object CarouselRec extends Scenario {

  override var scenarioType = ScenarioType.carousel
  var licenseCode: String = _
  var noLiceseSceneBid: Seq[String] = _
  var vodVersion: String = _

  override protected def run(): Unit = {

    getArgs()

    println(today)

    println(license)

    println(vodVersion)

    println(licenseCode)

    println(noLiceseSceneBid)

    noLiceseSceneBid.foreach(println)

    val df = getOrignalSubId(yesterday, license, vodVersion).limit(10)

    printDf("df", df)
  }

  def getArgs() = {

    licenseCode = argsMap("licenseCode").trim

    noLiceseSceneBid = argsMap("noLiceseSceneBid").split(",").map(i => i.trim)

    vodVersion = argsMap("vodVersion").trim
  }

  def printDf(df_name: String, df: DataFrame) = {

    println("—————————\n" * 2)
    println(df_name)
    println("—————————\n")
    df.show(false)
    println("—————————\n")
    df.printSchema()
    println("—————————\n" * 2)
  }
}
