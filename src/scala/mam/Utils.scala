package mam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.directory.shared.kerberos.codec.krbCredInfo.actions.StoreStartTime
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{length, udf}

import scala.collection.immutable.ListMap
import scala.collection.mutable

object Utils {

  // dataFrame信息打印函数
  def udfPrintDf = udf(printDf _)
  def printDf(df_name:String, df:DataFrame) = {

    println("_____________________\n"*2)
    println(df_name)
    println("_____________________\n")
    df.show(false)
    println("_____________________\n")
    df.printSchema()
    println("_____________________\n"*2)

  }

  def udfPrintArray = udf(printArray _)
  def printArray(array_name:String, array_self:Array[Row]) = {

    println("_____________________\n"*2)
    println(array_name)
    println("_____________________\n")
    array_self.take(10).foreach(println)
    println("_____________________\n"*2)

  }



  //orderProcess
  def udfChangeDateFormat=udf(changeDateFormat _)   //实名函数的注册 要在后面加 _(
  def changeDateFormat(date:String)= {
    if(date=="NULL"){
      "NULL"
    }else{
      //
      try{
        val sdf=new SimpleDateFormat("yyyyMMddHHmmSS")
        val dt:Long=sdf.parse(date).getTime()
        val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt)
        new_time
      }
      catch{
        case e: Exception =>{
          "NULL"
        }
      }
    }

  }
  //Long类型转换成时间
  def udfLongToDateTime = udf(longToDateTime _)
  def longToDateTime(time: Long) = {
    val newTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time*1000)
    newTime
  }


  def udfLongToTimestamp=udf(longToTimestamp _)
  def longToTimestamp(time:String)={
    if(time=="NULL")
    {
      "NULL"
    }
    if(time.length<10){
      "NULL"
    }
    else {
      val time_long = time.toLong
      val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)
      new_time
    }
  }




  def udfGetString=udf(getString _)
  def getString(features:Vector[Double])={
    features.mkString(",")
  }

  def udfAddOrderStatus = udf(code)
  def code = (arg: String) => {
    if (arg.getClass.getName == "java.lang.String") 1 else 0
  }


  def udfAddSuffix = udf(addSuffix _)
  def addSuffix(playEndTime:String) = {
    playEndTime + " 00:00:00"
  }


  //工具函数，计算一个日期加上几天后的日期
  def calDate(date:String,days:Int):String={
    val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val dt=sdf.parse(date);
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.DATE,days)//日期加天
    val dt1=rightNow.getTime()
    val reStr = sdf.format(dt1)
    return reStr
  }


  //计算日期相差的天数
  def udfGetDays= udf(getDays _)
  def getDays(date:String,now:String)={
    if(date==null){
      -1
    }else{
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
      val d1=sdf.parse(now)
      var d2=sdf.parse(now)
      if(date.length<19){
        d2=sdf.parse(date+" 00:00:00")
      }else{
        d2=sdf.parse(date)
      }
      var daysBetween=0
      if(now>date){
        daysBetween=((d1.getTime()-d2.getTime()+1000000)/(60*60*24*1000)).toInt
      }else{
        daysBetween=((d2.getTime()-d1.getTime()+1000000)/(60*60*24*1000)).toInt
      }
      daysBetween

    }

  }

  //根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
  def udfFillDiscountDescription = udf(fillDiscountDescription _)
  def fillDiscountDescription(resourceType:Double, timeValidity:Int):String={
    var dis = ""
    if (resourceType == 0.0){
      dis = "单点"
    }else{
      if (timeValidity <= 31) {
        dis = "包月"
      }else if (timeValidity > 31 && timeValidity < 180) {
        dis = "包季"
      }else if(timeValidity >= 180 && timeValidity < 360 ){
        dis =  "包半年"
      }else if(timeValidity >= 360){
        dis = "包年"
      }
    }
    return dis

  }

//  创建时间与生效时间的计算 返回是否保留的标记
  def udfGetKeepSign = udf(getKeepSign _)
  def getKeepSign(creationTime:String, startTime: String):Int={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val d1 = sdf.parse(creationTime)
    val d2 = sdf.parse(startTime)

    if (d1.getTime() <= d2.getTime() + 60000){  //创建时间小于生效时间加1min
       1
    } else{
       0
    }

  }



  //类似于计算wordcount
  def udfGetLabelAndCount=udf(getLabelAndCount _)
  def getLabelAndCount(array:mutable.WrappedArray[String])={
    val group_data = array.map(item=>(item,1)).groupBy(item=>item._1)
    val res = group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //res
    ListMap(res.toSeq.sortWith(_._2 >_._2) :_ *)
  }

  def udfGetLabelAndCount2=udf(getLabelAndCount2 _)
  def getLabelAndCount2(array:mutable.WrappedArray[mutable.WrappedArray[String]])={
    //可变长数组
    var res:Array[String]=Array()
    for(a <- array)
    {
      //a.foreach(item=>res.addString(new StringBuilder(item)))
      res=res.union(a)
    }
    val group_data=res.map(item=>(item,1)).groupBy(item=>item._1)
    val result=group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //result
    ListMap(result.toSeq.sortWith(_._2 >_._2) :_ *)
  }

//  def udfGetSign = udf(getSign _)
//  def getSign(data: , timeGapCol: String):Int = {
//    for(a <- 0 to 10){
//      println(a)
//    }
//
//    return 1
//  }


}
