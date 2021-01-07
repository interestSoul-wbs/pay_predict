package rs.common

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.streaming.Time

object DateTimeTool {
  def getDateTimeString(time: Time): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date(time.milliseconds))
  }

  def getCurrentUnixStamp: Long = {
    System.currentTimeMillis()./(1000)
  }

  def getDateTimeString: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date())
  }

  def getRealSysDateTimeString: String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date())
  }

  def getDateTimeCodeFormatSecond(time: Time): Long = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date(time.milliseconds)).toLong
  }

  def getDateTimeCodeFormatSecond: Long = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date()).toLong
  }

  def getDateTimeCodeFormatDay(time: Time): Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date(time.milliseconds)).toLong
  }

  def getDateTimeCodeFormatDay: Long = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date()).toLong
  }

  def getDateTimeCodeFormatOnlyHour: Long = {
    val sdf = new SimpleDateFormat("HH")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"))
    sdf.format(new java.util.Date()).toLong
  }

  /**
    * @param days: 3 means 3 days later; -3 means 3 days ago.
    */
  def getDaysAgoAfter(dateTimeStringEight: String, days: Int): String = {

    val sdf = new SimpleDateFormat("yyyyMMdd")
    val dt = sdf.parse(dateTimeStringEight)
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.DATE, days)
    val dt1 = rightNow.getTime()
    val reStr = sdf.format(dt1)

    reStr
  }
}
