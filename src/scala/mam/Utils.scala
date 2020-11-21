package mam

import java.text.SimpleDateFormat
import java.util.Calendar

import com.github.nscala_time.time.Imports._
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler, Word2Vec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._

object Utils {


  def printDf(df_name: String, df: DataFrame) = {
    /**
      * @description dataframe信息输出
      * @author wx
      * @param [df_name ]
      * @param [df ]
      * @return { @link void }
      **/
    println("_____________________\n" * 2)
    println(df_name)
    println("_____________________\n")
    df.show(false)
    println("_____________________\n")
    df.printSchema()
    println("_____________________\n" * 2)

  }

  def printArray(array_name: String, array_self: Array[Row]) = {

    println("_____________________\n" * 2)
    println(array_name)
    println("_____________________\n")
    array_self.take(10).foreach(println)
    println("_____________________\n" * 2)

  }


  //orderProcess
  /**
    * @author wj
    * @param [date ]
    * @return java.lang.String
    * @describe 修改日期时间的格式
    */
  def udfChangeDateFormat = udf(changeDateFormat _) //实名函数的注册 要在后面加 _(

  def changeDateFormat(date: String) = {

    val sdf = new SimpleDateFormat("yyyyMMddHHmmSS")

    val dt: Long = sdf.parse(date).getTime()

    val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(dt)

    new_time
  }

  //  def udfChangeDateFormat = udf(changeDateFormat _) //实名函数的注册 要在后面加 _(
  //
  //  def changeDateFormat(date: String) = {
  //    /**
  //      * @author wj
  //      * @param [date ]
  //      * @return java.lang.String
  //      * @describe 修改日期时间的格式
  //      */
  //    if (date == "NULL") {
  //      "0000-00-00 00:00:00"
  //    } else {
  //      //
  //      try {
  //        val sdf = new SimpleDateFormat("yyyyMMddHHmmSS")
  //        val dt: Long = sdf.parse(date).getTime()
  //        val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS").format(dt)
  //        new_time
  //      }
  //      catch {
  //        case e: Exception => {
  //          "NULL"
  //        }
  //      }
  //    }
  //
  //  }

  def udfLongToDateTime = udf(longToDateTime _)

  def longToDateTime(time: Long) = {
    /**
      * @description Long类型转换成时间格式
      * @author wx
      * @param [time ]
      * @return { @link java.lang.String }
      **/
    val newTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time * 1000)
    newTime
  }

  /**
    * @author wj
    * @param [time ]
    * @return java.lang.String
    * @describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
    */
  def udfLongToTimestamp = udf(longToTimestamp _)

  def longToTimestamp(time: String) = {

    val time_long = time.toLong

    val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)

    new_time
  }

  //  def udfLongToTimestamp = udf(longToTimestamp _)
  //
  //  def longToTimestamp(time: String) = {
  //    /**
  //      * @author wj
  //      * @param [time ]
  //      * @return java.lang.String
  //      * @describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
  //      */
  //    if (time == "NULL") {
  //      "NULL"
  //    }
  //    if (time.length < 10) {
  //      "NULL"
  //    }
  //    else {
  //      val time_long = time.toLong
  //      val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)
  //      new_time
  //    }
  //  }

  def udfLongToTimestampV2 = udf(longToTimestampV2 _)

  def longToTimestampV2(time: String) = {
    /**
      * @author wj
      * @param [time ]
      * @return java.lang.String
      * @describe 将long类型的时间戳，转化为yyyy-MM-dd HH:mm:ss的字符串
      */
    if (time == "NULL") {
      null
    }
    if (time.length < 10) {
      null
    }
    else {
      val time_long = time.toLong
      val new_time: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time_long * 1000)
      new_time
    }
  }

  def udfAddOrderStatus = udf(addOrderStatus _)

  def addOrderStatus(arg: String) = {

    if (arg.getClass.getName == "java.lang.String") 1 else 0
  }

  def udfAddSuffix = udf(addSuffix _)

  def addSuffix(playEndTime: String) = {

    playEndTime + " 00:00:00"
  }


  //工具函数，计算一个日期加上几天后的日期
  def calDate(date: String, days: Int): String = {

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val dt = sdf.parse(date);
    val rightNow = Calendar.getInstance()
    rightNow.setTime(dt)
    rightNow.add(Calendar.DATE, days)
    //日期加天
    val dt1 = rightNow.getTime()
    val reStr = sdf.format(dt1)
    return reStr
  }


  //计算日期相差的天数
  //  def udfGetDays = udf(getDays _)
  //
  //  def getDays(date: String, now: String) = {
  //
  //    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
  //
  //    val d1 = sdf.parse(now)
  //
  //    val d2 = sdf.parse(date)
  //
  //    var daysBetween = 0
  //    if (now > date) {
  //      daysBetween = ((d1.getTime() - d2.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
  //    } else {
  //      daysBetween = ((d2.getTime() - d1.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
  //    }
  //
  //    daysBetween
  //  }

  def udfGetDays = udf(getDays _)

  def getDays(date: String, now: String) = {

    if (date == "NULL") {
      -1
    } else if (date == null) {
      -1
    } else {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
      val d1 = sdf.parse(now)
      var d2 = sdf.parse(now)
      if (date.length < 19) {
        d2 = sdf.parse(date + " 00:00:00")
      } else {
        d2 = sdf.parse(date)
      }
      var daysBetween = 0
      if (now > date) {
        daysBetween = ((d1.getTime() - d2.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
      } else {
        daysBetween = ((d2.getTime() - d1.getTime() + 1000000) / (60 * 60 * 24 * 1000)).toInt
      }
      daysBetween
    }
  }

  //根据 time_validity 和 resource_type 填充order中 discount_description 为 null的数值
  def udfFillDiscountDescription = udf(fillDiscountDescription _)

  def fillDiscountDescription(resourceType: Double, timeValidity: Int): String = {
    /**
      * @description 订单打折描述填充
      * @author wx
      * @param [resourceType ] 订单资源类型
      * @param [timeValidity ] 订单有效时长
      * @return { @link java.lang.String }
      **/
    var dis = ""
    if (resourceType == 0.0) {
      dis = "单点"
    } else {
      if (timeValidity <= 31) {
        dis = "包月"
      } else if (timeValidity > 31 && timeValidity < 180) {
        dis = "包季"
      } else if (timeValidity >= 180 && timeValidity < 360) {
        dis = "包半年"
      } else if (timeValidity >= 360) {
        dis = "包年"
      }
    }
    return dis

  }

  def udfGetKeepSign = udf(getKeepSign _)

  def getKeepSign(creationTime: String, startTime: String): Int = {
    /**
      * @description 创建时间与生效时间的计算 返回是否保留的标记
      * @author wx
      * @param [creationTime ] 订单创建时间
      * @param [startTime ] 订单开始生效时间
      * @return { @link int }
      **/
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")
    val d1 = sdf.parse(creationTime)
    val d2 = sdf.parse(startTime)

    if (d1.getTime() <= d2.getTime() + 60000) { //创建时间小于生效时间加1min
      1
    } else {
      0
    }

  }


  //类似于计算wordcount
  def udfGetLabelAndCount = udf(getLabelAndCount _)

  def getLabelAndCount(array: mutable.WrappedArray[String]) = {
    /**
      * @author wj
      * @param [array ]
      * @return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
      * @describe 类似于计算wordcount,计算每个用户看过的视频中每种一级分类和它对应的个数
      */
    val group_data = array.map(item => (item, 1)).groupBy(item => item._1)
    val res = group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //res
    ListMap(res.toSeq.sortWith(_._2 > _._2): _ *)
  }

  def udfGetLabelAndCount2 = udf(getLabelAndCount2 _)

  def getLabelAndCount2(array: mutable.WrappedArray[mutable.WrappedArray[String]]) = {
    /**
      * @author wj
      * @param [array ]
      * @return scala.collection.immutable.ListMap<java.lang.String,java.lang.Object>
      * @describe 计算每个用户看过的视频中每种二级分类或者标签以及它对应的个数
      */
    //可变长数组
    var res: Array[String] = Array()
    for (a <- array) {
      //a.foreach(item=>res.addString(new StringBuilder(item)))
      res = res.union(a)
    }
    val group_data = res.map(item => (item, 1)).groupBy(item => item._1)
    val result = group_data.map(tp => {
      val list: mutable.WrappedArray[(String, Int)] = tp._2
      val counts: mutable.WrappedArray[Int] = list.map(t => t._2)
      (tp._1, counts.sum)
    })
    //result
    ListMap(result.toSeq.sortWith(_._2 > _._2): _ *)
  }

  //  def udfBreak = udf(break _)
  //
  //  def break(array: Object, index: Int) = {
  //    /**
  //      * @author wj
  //      * @param [array , index]
  //      * @return double
  //      * @description 将一个vector拆分成多个列
  //      */
  //    val vectorString = array.toString
  //    vectorString.substring(1, vectorString.length - 1).split(",")(index).toDouble
  //    //(Vector)
  //
  //  }

  def udfFillPreference = udf(fillPreference _)

  def fillPreference(prefer: Map[String, Int], offset: Int) = {
    /**
      * @author wj
      * @param [prefer , offset]
      * @return java.lang.String
      * @description 新添加一列，将该列填充为prefer中的标签
      */
    if (prefer == null) {
      null
    } else {
      val mapArray = prefer.toArray
      if (mapArray.length > offset - 1) {
        mapArray(offset - 1)._1
      } else {
        null
      }

    }

  }

  def udfFillPreferenceIndex = udf(fillPreferenceIndex _)

  def fillPreferenceIndex(prefer: String, mapLine: String) = {
    /**
      * @author wj
      * @param [prefer , mapLine]
      * @return scala.Option<java.lang.Object>
      * @description 将udfFillPreference填充的标签，转化为标签对应的index
      */
    if (prefer == null) {
      null
    } else {
      var tempMap: Map[String, Int] = Map()
      var lineIterator1 = mapLine.split(",")
      //迭代打印所有行
      lineIterator1.foreach(m => tempMap += (m.split(" -> ")(0) -> m.split(" -> ")(1).toInt))
      tempMap.get(prefer)
    }
  }

  /**
    * 判断一个时间是不是格式化的时间 - 例：2014-06-11 00:00:00
    *
    * @return
    */
  def udfIsFormattedTime = udf(isFormattedTime _)

  def isFormattedTime(formatted_time: String) = {

    var result = 0

    val pattern_1 = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$".r

    val tmp_result = pattern_1 findFirstIn formatted_time

    tmp_result match {
      case None => result = 0
      case _ => result = 1
    }

    result
  }

  /**
    * 判断一个字符串是不是 数字，至少有1位数
    *
    * @return
    */
  def udfIsOnlyNumber = udf(isOnlyNumber _)

  def isOnlyNumber(str: String) = {

    var result = 0

    val pattern_1 = """^\d+$""".r

    val tmp_result = pattern_1 findFirstIn str

    tmp_result match {
      case None => result = 0
      case _ => result = 1
    }

    result
  }

  /**
    * 判断一个时间是不是 LongType - 10位数字 的时间 - 例：1425939527
    *
    * @return
    */
  def udfIsLongTypeTimePattern1 = udf(isLongTypeTimePattern1 _)

  def isLongTypeTimePattern1(longtype_time: String) = {

    var result = 0

    val pattern_1 = """^\d{10}$""".r

    val tmp_result = pattern_1 findFirstIn longtype_time

    tmp_result match {
      case None => result = 0
      case _ => result = 1
    }

    result
  }

  /**
    * 判断一个时间是不是 LongType - 14位数字 的时间 - 例：1425939527
    *
    * @return
    */
  def udfIsLongTypeTimePattern2 = udf(isLongTypeTimePattern2 _)

  def isLongTypeTimePattern2(longtype_time: String) = {

    var result = 0

    val pattern_1 = """^\d{14}$""".r

    val tmp_result = pattern_1 findFirstIn longtype_time

    tmp_result match {
      case None => result = 0
      case _ => result = 1
    }

    result
  }


  def module(vec: ArrayBuffer[Int]): Double = {
    // math.sqrt( vec.map(x=>x*x).sum )
    math.sqrt(vec.map(math.pow(_, 2)).sum)
  }

  /**
    * 求两个向量的内积
    *
    * @param v1
    * @param v2
    */
  def innerProduct(v1: ArrayBuffer[Int], v2: ArrayBuffer[Int]): Double = {
    val arrayBuffer = ArrayBuffer[Double]()
    for (i <- 0 until v1.length - 1) {
      arrayBuffer.append(v1(i) * v2(i))
    }
    arrayBuffer.sum
  }

  /**
    * 求两个向量的余弦值
    *
    * @param v1
    * @param v2
    */
  def cosvec(v1: ArrayBuffer[Int], v2: ArrayBuffer[Int]): Double = {
    val cos = innerProduct(v1, v2) / (module(v1) * module(v2))
    if (cos <= 1) cos else 1.0
  }

  def udfCalFirstCategorySimilarity = udf(calFirstCategorySimilarity _)

  def calFirstCategorySimilarity(category: String, preference: Map[String, Int], videoFirstCategoryString: String) = {
    var videoFirstCategoryMap: Map[String, Int] = Map()
    for (elem <- videoFirstCategoryString.split(",")) {
      videoFirstCategoryMap += (elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
    }

    if (category == null || preference == null) {
      0.0
    } else {
      var categoryArray = new ArrayBuffer[Int](videoFirstCategoryMap.size)
      var preferenceArray = new ArrayBuffer[Int](videoFirstCategoryMap.size)
      for (i <- 0 to videoFirstCategoryMap.size - 1) {
        categoryArray.append(0)
        preferenceArray.append(0)
      }
      for (elem <- preference.keys.toList) {
        var index = videoFirstCategoryMap.getOrElse(elem, 0)
        if (index >= preferenceArray.length) {

        } else {
          preferenceArray(index) = preference.getOrElse(elem, 0)
        }
      }

      var index2 = videoFirstCategoryMap.getOrElse(category, 0)
      if (index2 >= preferenceArray.length) {

      } else {
        categoryArray(index2) = 1
      }

      cosvec(categoryArray, preferenceArray)
    }

  }


  def udfCalSecondCategorySimilarity = udf(calSecondCategorySimilarity _)

  def calSecondCategorySimilarity(category: mutable.WrappedArray[String], preference: Map[String, Int], videoSecondCategoryString: String) = {
    var videoSecondCategoryMap: Map[String, Int] = Map()
    for (elem <- videoSecondCategoryString.split(",")) {
      videoSecondCategoryMap += (elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
    }
    if (category == null || preference == null) {
      0.0
    } else {
      var categoryArray = new ArrayBuffer[Int](videoSecondCategoryMap.size)
      var preferenceArray = new ArrayBuffer[Int](videoSecondCategoryMap.size)
      for (i <- 0 to videoSecondCategoryMap.size - 1) {
        categoryArray.append(0)
        preferenceArray.append(0)
      }
      for (elem <- preference.keys.toList) {
        var index = videoSecondCategoryMap.getOrElse(elem, 0)
        if (index >= preferenceArray.length) {

        } else {
          preferenceArray(index) = preference.getOrElse(elem, 0)
        }
      }

      for (elem <- category) {
        var index = videoSecondCategoryMap.getOrElse(elem, 0)
        if (index >= preferenceArray.length) {

        } else {
          categoryArray(index) = 1
        }
      }
      cosvec(categoryArray, preferenceArray)
    }

  }

  def udfCalLabelSimilarity = udf(calLabelSimilarity _)

  def calLabelSimilarity(category: mutable.WrappedArray[String], preference: Map[String, Int], labelString: String) = {
    var labelMap: Map[String, Int] = Map()
    for (elem <- labelString.split(",")) {
      labelMap += (elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
    }
    if (category == null || preference == null) {
      0.0
    } else {
      var categoryArray = new ArrayBuffer[Int](labelMap.size)
      var preferenceArray = new ArrayBuffer[Int](labelMap.size)
      for (i <- 0 to labelMap.size - 1) {
        categoryArray.append(0)
        preferenceArray.append(0)
      }
      //println(categoryArray.length+" "+preferenceArray.length)
      for (elem <- preference.keys.toList) {
        var index = labelMap.getOrElse(elem, 0)
        if (index >= preferenceArray.length) {

        } else {
          preferenceArray(index) = preference.getOrElse(elem, 0)
        }
      }

      for (elem <- category) {
        var index = labelMap.getOrElse(elem, 0)
        if (index >= preferenceArray.length) {

        } else {
          categoryArray(index) = 1
        }
      }
      // println(categoryArray)
      // println(preferenceArray)
      cosvec(categoryArray, preferenceArray)
    }

  }

  def getCategoryMap(df_category: DataFrame) = {

    var df_category_map: Map[String, Int] = Map()

    val conList = df_category.collect()

    for (elem <- conList) {
      val s = elem.toString()
      df_category_map += (s.substring(1, s.length - 1).split("\t")(1) -> s.substring(1, s.length - 1).split("\t")(0).toInt)
    }

    df_category_map
  }

  def getFilteredColList(df_user_profile: DataFrame) = {

    val colTypeList = df_user_profile.dtypes.toList

    val colList = ArrayBuffer[String]()

    for (elem <- colTypeList) {
      if (elem._2.equals("IntegerType") || elem._2.equals("DoubleType")
        || elem._2.equals("LongType") || elem._2.equals("StringType")) {
        colList.append(elem._1)
      }
    }

    colList.toList
  }

  def getNumerableColsSeq(df_result_tmp_1: DataFrame) = {

    val colTypeList = df_result_tmp_1.dtypes.toList

    val colList = ArrayBuffer[String]()

    colList.+=(Dic.colUserId, Dic.colVideoId)

    for (elem <- colTypeList) {
      if (elem._2.equals("IntegerType") || elem._2.equals("DoubleType") || elem._2.equals("LongType")) {
        colList.append(elem._1)
      }
    }

    colList.-=(Dic.colIsSingle, Dic.colIsTrailers, Dic.colIsPaid)

    colList.toList
  }


  def getPredictUsersLabel(df_processed_order: DataFrame, predict_time: String) = {

    val end_date = getDaysAfter(predict_time, 14)

    val df_result = df_processed_order
      .filter(col(Dic.colCreationTime).gt(lit(predict_time)) && col(Dic.colCreationTime).lt(lit(end_date)))
      .withColumn(Dic.colRank, row_number().over(Window.partitionBy(col(Dic.colUserId)).orderBy(col(Dic.colOrderStatus).desc)))
      .filter(col(Dic.colOrderStatus).>(lit(1)) && col(Dic.colRank).===(lit(1)))
      .select(
        col(Dic.colUserId),
        lit(1).as(Dic.colOrderStatus))

    df_result
  }

  def getDaysAfter(date_now: String, n: Int) = {

    val date_now_formatted = DateTime.parse(date_now, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    val date_after = (date_now_formatted + n.days).toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:SS"))

    date_after
  }

  case class splitVecToDoubleDfAndNewCols(df_result_with_vec_cols: DataFrame, new_cols_array: Array[String])

  /**
    *
    * @param df_raw_video_dict
    * @return
    */
  def splitVecToDouble(df_raw_video_dict: DataFrame, vec_col: String, vectorDimension: Int, prefix_of_separate_vec_col: String) = {

    // 将 vec_col 切分后的df
    val df_result = df_raw_video_dict
      .select(
        col("*")
          +: (0 until vectorDimension).map(i => col(vec_col).getItem(i).as(s"$prefix_of_separate_vec_col" + s"_$i")): _*)

    // 将 vec_col 切分后，新出现的 col name array
    val new_cols_array = ArrayBuffer[String]()

    for (i <- 0 until vectorDimension) {
      new_cols_array.append(s"$prefix_of_separate_vec_col" + s"_$i")
    }

    splitVecToDoubleDfAndNewCols(df_result, new_cols_array.toArray)
  }

  /**
    *
    * @param df_raw_video_dict
    * @return
    */
  def splitVecToDoubleRanlPart(df_raw_video_dict: DataFrame, selected_id_col_1: String, selected_id_col_2: String,
                               vec_col: String, vectorDimension: Int, prefix_of_separate_vec_col: String) = {

    val df_video_dict = df_raw_video_dict
      .select(
        col(selected_id_col_1) +:
          col(selected_id_col_2) +:
          (0 until vectorDimension).map(i => col(vec_col).getItem(i).as(s"$prefix_of_separate_vec_col" + s"_$i")): _*)

    df_video_dict
  }

  /**
    *
    * @return
    */
  def udfVectorToArray = udf(vectorToArray _)

  def vectorToArray(col_vector: Vector) = {
    col_vector.toArray
  }

  def getPlayList(df_plays: DataFrame, now: String) = {

    val df_play_list = df_plays
      .withColumn(Dic.colPlayMonth, substring(col(Dic.colPlayEndTime), 0, 7))
      .filter(col(Dic.colPlayEndTime).<(lit(now)))
      .groupBy(col(Dic.colUserId), col(Dic.colPlayMonth))
      .agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoList))

    df_play_list
  }

  /**
    * Transform Spark vectors to double.
    *
    * @return
    */
  def udfBreak = udf(break _)

  def break(array: Object, index: Int) = {

    val vectorString = array.toString

    vectorString.substring(1, vectorString.length - 1).split(",")(index).toDouble
  }

  /**
    * Word2vec training.
    */
  def getVector(df_plays_list: DataFrame, vector_dimension: Int = 64, window_size: Int = 10, min_count: Int = 5,
                max_length: Int = 30, max_iteration: Int = 1,
                input_col: String = Dic.colVideoList, output_col: String = Dic.colResult) = {

    val w2vModel = new Word2Vec()
      .setInputCol(input_col)
      .setOutputCol(output_col)
      .setVectorSize(vector_dimension)
      .setWindowSize(window_size)
      .setMinCount(min_count)
      .setMaxSentenceLength(max_length)
      .setMaxIter(max_iteration)

    val model = w2vModel.fit(df_plays_list)

    val df_raw_video_dict = model
      .getVectors
      .select(
        col(Dic.colWord).as(Dic.colVideoId),
        udfVectorToArray(
          col(Dic.colVector)).as(Dic.colVector))

    val splitVecToDoubleDfAndNewCols = splitVecToDouble(df_raw_video_dict, Dic.colVector, vector_dimension, "v")

    val df_video_dict = splitVecToDoubleDfAndNewCols.df_result_with_vec_cols
      .select(Array(Dic.colVideoId).++(splitVecToDoubleDfAndNewCols.new_cols_array).map(col): _*)

    df_video_dict
  }


  def scaleData(df_orginal: DataFrame, excluded_cols: Array[String]) = {

    val all_original_cols = df_orginal.columns

    val input_cols = all_original_cols.diff(excluded_cols)

    val final_df_schema = excluded_cols.++(input_cols)

    val assembler = new VectorAssembler()
      .setInputCols(input_cols)
      .setOutputCol(Dic.colFeatures)

    val df_output = assembler.transform(df_orginal)

    val minMaxScaler = new MinMaxScaler()
      .setInputCol(Dic.colFeatures)
      .setOutputCol(Dic.colScaledFeatures)
      .fit(df_output)

    val df_scaled_data = minMaxScaler.transform(df_output)
      .withColumn(Dic.colVector, udfVectorToArray(col(Dic.colScaledFeatures)))

    val splitVecToDoubleDfAndNewCols = splitVecToDouble(df_scaled_data, Dic.colVector, input_cols.length, "f")

    val df_result = splitVecToDoubleDfAndNewCols.df_result_with_vec_cols
      .select(excluded_cols.++(splitVecToDoubleDfAndNewCols.new_cols_array).map(col): _*)
      .toDF(final_df_schema: _*)

    df_result
  }
}
