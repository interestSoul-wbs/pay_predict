package mam

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions.udf

import scala.collection.immutable.ListMap
import scala.collection.mutable

object Utils {
  //orderProcess
  def udfChangeDateFormat=udf(changeDateFormat _)
  def changeDateFormat(date:String)= {
    /**
    *@author wj
    *@param [date]
    *@return java.lang.String
    *@describe 修改日期时间的格式
    */
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

  def udfLongToTimestamp=udf(longToTimestamp _)
  def longToTimestamp(time:String)={
    /**
    *@author wj
    *@param [time]
    *@return java.lang.String
    *@describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
    */
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
    /**
    *@author wj
    *@param [features]
    *@return java.lang.String
    *@describe  转化为字符串
    */
    features.mkString(",")
  }

  def udfAddOrderStatus = udf(addOrderStatus _)
  def addOrderStatus(arg: String) = {
   /**
   *@author wj
   *@param [arg]
   *@return int
   *@describe 主要在构造训练集的时候创建标签
   */
    if (arg.getClass.getName == "java.lang.String") 1 else 0
  }

  def udfAddSuffix=udf(addSuffix _)
  def addSuffix(playEndTime:String)={
    /**
    *@author wj
    *@param [playEndTime]
    *@return java.lang.String
    *@describe 在日期后面添加后缀，主要用在PlayProcess文件里面
    */
    playEndTime+" 00:00:00"
  }



  def calDate(date:String,days:Int):String={
    /**
    *@author wj
    *@param [date, days]
    *@return java.lang.String
    *@describe 非udf函数，计算一个日期加上几天后的日期
    */
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
   /**
   *@author wj
   *@param [date, now]
   *@return int
   *@describe 计算两个日期相差的天数
   */
    if(date==null){
      -1
    }else{
      val sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
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
  //类似于计算wordcount
  def udfGetLabelAndCount=udf(getLabelAndCount _)
  def getLabelAndCount(array:mutable.WrappedArray[String])={
    /**
    *@author wj
    *@param [array]
    *@return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
    *@describe 类似于计算wordcount,计算每个用户看过的视频中每种一级分类和它对应的个数
    */
    val group_data=array.map(item=>(item,1)).groupBy(item=>item._1)
    val res=group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //res
    ListMap(res.toSeq.sortWith(_._2 >_._2) :_ *)
  }
  def udfGetLabelAndCount2=udf(getLabelAndCount2 _)
  def getLabelAndCount2(array:mutable.WrappedArray[mutable.WrappedArray[String]])={
    /**
    *@author wj
    *@param [array]
    *@return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
     *@describe    计算每个用户看过的视频中每种二级分类或者标签以及它对应的个数
    */
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
  

}
