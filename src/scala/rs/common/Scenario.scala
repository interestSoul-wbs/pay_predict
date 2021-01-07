package rs.common

import com.github.nscala_time.time.Imports._
import org.apache.log4j.{Level, Logger}
import DateTimeTool._

trait Scenario {
  var scenarioType: ScenarioType.Value
  var date: DateTime = _
  var timeCode: String = _
  var today: String = _
  var yesterday: String = _
  var beforeYesterday: String = _
  var sevenDaysBefore: String = _
  var xmlPath: String = _
  var argsMap: Map[String, String] = _
  var license: String = _
  var args: Array[String] = _
  var partitionMap: Map[String, String] = _
  val logger: Logger = Logger.getLogger(this.getClass.getName)
  var realSystemDate: String = _
  var realSystemDateOneDayAgo: String = _

  def main(args: Array[String]): Unit = {

    SparkSessionInit.init()

    val startTime = System.currentTimeMillis()
    println(s"${DateTime.now.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))} MYINFO ${scenarioType.toString} BEGIN")
    // input
    this.args = args
    logger.setLevel(Level.WARN)
    if (args.length < 2) {
      logger.warn("this is warn")
      throw new Exception("ERROR : the count of args is less than 2!")
    }
    xmlPath = args(1)
    val dateTimeString = args(0)
    dateTimeString.length match {
      case 8 => date = DateTime.parse(dateTimeString.substring(0, 8), DateTimeFormat.forPattern("yyyyMMdd"))
      case 10 => date = DateTime.parse(dateTimeString.substring(0, 10), DateTimeFormat.forPattern("yyyyMMddHH"))
      case 12 => date = DateTime.parse(dateTimeString.substring(0, 12), DateTimeFormat.forPattern("yyyyMMddHHmm"))
      case 14 => date = DateTime.parse(dateTimeString.substring(0, 14), DateTimeFormat.forPattern("yyyyMMddHHmmss"))
      case _ =>
        throw new Exception("ERROR : Illegal time string!")
    }
    // date
    timeCode = date.toString(DateTimeFormat.forPattern("yyyyMMddHHmm")).substring(8, 12)
    today = date.toString(DateTimeFormat.forPattern("yyyyMMdd"))
    yesterday = (date - 1.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    beforeYesterday = (date - 2.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))
    sevenDaysBefore = (date - 7.days).toString(DateTimeFormat.forPattern("yyyyMMdd"))

    realSystemDate = DateTimeTool.getRealSysDateTimeString
    realSystemDateOneDayAgo = DateTimeTool.getDaysAgoAfter(realSystemDate, -1)

    // rec
    val rec = new Recommendation().analyze(xmlPath, scenarioType)
    license = rec.license
    CurrentLicense.init(license)
    argsMap = rec.argsMap

    // main
    run()

    val endTime = System.currentTimeMillis()
    println(s"${DateTime.now.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))} MYINFO ${scenarioType.toString} SUCCESS")
    println(s"${DateTime.now.toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))} MYINFO Duration:${(endTime - startTime)./(1000)}s")
  }

  protected def run()

}
