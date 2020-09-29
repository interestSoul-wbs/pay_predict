package predict.common

import mam.Dic
import mam.Utils.udfLongToTimestamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Set} // 可以在任何地方引入 可变集合

object MediasProcess {

  //将字符串属性转化为

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("MediasProcess")
      .master("local[6]")
      .getOrCreate()

    val schema= StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colVideoTitle, StringType),
        StructField(Dic.colVideoOneLevelClassification, StringType),
        StructField(Dic.colVideoTwoLevelClassificationList, StringType),
        StructField(Dic.colVideoTagList, StringType),
        StructField(Dic.colDirectorList, StringType),
        StructField(Dic.colActorList, StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField(Dic.colReleaseDate, StringType),
        StructField(Dic.colStorageTime, StringType),
        //视频时长
        StructField(Dic.colVideoTime, StringType),
        StructField(Dic.colScore, StringType),
        StructField(Dic.colIsPaid, StringType),
        StructField(Dic.colPackageId, StringType),
        StructField(Dic.colIsSingle, StringType),
        //是否片花
        StructField(Dic.colIsTrailers, StringType),
        StructField(Dic.colSupplier, StringType),
        StructField(Dic.colIntroduction, StringType)
      )
    )
    //val hdfsPath="hdfs:///pay_predict"
    val hdfsPath=""
    import org.apache.spark.sql.functions._
    val mediasRawPath=hdfsPath+"data/predict/common/raw/medias/medias.txt"
    val mediasProcessedPath=hdfsPath+"data/predict/common/processed/mediastemp"
    val videoFirstCategoryTempPath=hdfsPath+"data/predict/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath=hdfsPath+"data/predict/common/processed/videosecondcategorytemp.txt"
    val labelTempPath=hdfsPath+"data/predict/common/processed/labeltemp.txt"///pay_predict/data/train/common/processed
    val df = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(mediasRawPath)
    val df1=df.withColumn(Dic.colDirectorList,from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)))
           .withColumn(Dic.colVideoTwoLevelClassificationList,from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)))
           .withColumn(Dic.colVideoTagList,from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)))
           .withColumn(Dic.colActorList,from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)))
           .withColumn(Dic.colStorageTime,udfLongToTimestamp(col(Dic.colStorageTime)))
    val df2=df1.select(
      when(col(Dic.colVideoId)==="NULL",null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
      when(col(Dic.colVideoTitle)==="NULL",null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
      when(col(Dic.colVideoOneLevelClassification)==="NULL",null).otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
      col(Dic.colVideoTwoLevelClassificationList),
      col(Dic.colVideoTagList),
      col(Dic.colDirectorList),
      col(Dic.colActorList),
      when(col(Dic.colCountry)==="NULL",null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
      when(col(Dic.colLanguage)==="NULL",null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
      when(col(Dic.colReleaseDate)==="NULL",null).otherwise(col(Dic.colReleaseDate) ).as(Dic.colReleaseDate),
      when(col(Dic.colStorageTime)==="NULL",null).otherwise(col(Dic.colStorageTime )).as(Dic.colStorageTime),
      when(col(Dic.colVideoTime)==="NULL",Double.NaN).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
      when(col(Dic.colScore)==="NULL",Double.NaN).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
      when(col(Dic.colIsPaid)==="NULL",Double.NaN).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
      when(col(Dic.colPackageId)==="NULL",null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
      when(col(Dic.colIsSingle)==="NULL",Double.NaN).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
      when(col(Dic.colIsTrailers)==="NULL",Double.NaN).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
      when(col(Dic.colSupplier)==="NULL",null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
      when(col(Dic.colIntroduction)==="NULL",null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction)
    )


    val result_1=df2.select(collect_list(Dic.colVideoOneLevelClassification)).collect()
    val result_2=df2.select(collect_list(Dic.colVideoTwoLevelClassificationList)).collect()
    val result_3=df2.select(collect_list(Dic.colVideoTagList)).collect()
    val labelList=ListBuffer[String]()
    val firstList=ListBuffer[String]()
    val secondList=ListBuffer[String]()
    result_3.foreach(row=>{
      //println(row.get(0))
      val rs = row.getList(0)
      val mutableset = Set[String]()
      for (i <- 0 to rs.size() - 1) {
        val temp:mutable.WrappedArray[String]=rs.get(i)
        for(j<-0 to temp.length-1)
          mutableset.add(temp(j))
      }
      var label: Array[String] = mutableset.mkString(",").split(",")
      //val  file=new File()
      //val out = new PrintWriter(labelTempPath)
      for (i<-0 to label.length-1) {
          //out.println(i+"\t"+label(i))
        labelList.append(i+"\t"+label(i))
      }
      //out.close()

    })
    //val rs=mutable.WrappedArray[String]
    result_2.foreach(row=>{
     // println(row.get(0))
      val rs = row.getList(0)
      val mutableset = Set[String]()
      for (i <- 0 to rs.size() - 1) {
        val temp:mutable.WrappedArray[String]=rs.get(i)
        for(j<-0 to temp.length-1)
              mutableset.add(temp(j))
      }
      var level_two: Array[String] = mutableset.mkString(",").split(",")
      //val  file=new File(videoSecondCategoryTempPath)
     // val out = new PrintWriter(videoSecondCategoryTempPath)
      for (i<-0 to level_two.length-1) {
        //  out.println(i+"\t"+level_two(i))
        secondList.append(i+"\t"+level_two(i))

      }
      //out.close()

    })

    result_1.foreach(row=> {
      val rs = row.getList(0)
      val mutableset = Set[String]()
      for (i <- 0 to rs.size() - 1) {
        mutableset.add(rs.get(i))
      }
      var level_one: Array[String] = mutableset.mkString(",").split(",")
      //val  file=new File(videoFirstCategoryTempPath)
      //val out = new PrintWriter(videoFirstCategoryTempPath)
      for (i<-0 to level_one.length-1) {
         // out.println(i+"\t"+level_one(i))
        firstList.append(i+"\t"+level_one(i))
      }
      //out.close()
    })

    //df2.show()
    //println(labelList)
//    val writer1 = new PrintWriter(new File(labelTempPath))
//    for(elem<-labelList){
//      writer1.write(elem)
//    }
//    writer1.close()
//    val writer2 = new PrintWriter(new File(videoSecondCategoryTempPath))
//    for(elem<-secondList){
//      writer2.write(elem)
//    }
//    writer2.close()
//    val writer3 = new PrintWriter(new File(videoFirstCategoryTempPath))
//    for(elem<-firstList){
//      writer3.write(elem)
//    }
//    writer3.close()

    import spark.implicits._
    var labelCsv = labelList.toDF("content")
    labelCsv.coalesce(1).write.mode(SaveMode.Overwrite).option("header","false").csv(labelTempPath)
    var firstCsv = firstList.toDF("content")
    firstCsv.coalesce(1).write.mode(SaveMode.Overwrite).option("header","false").csv(videoFirstCategoryTempPath)
    var secondCsv = secondList.toDF("content")
    secondCsv.coalesce(1).write.mode(SaveMode.Overwrite).option("header","false").csv(videoSecondCategoryTempPath)




     df2.write.mode(SaveMode.Overwrite).format("parquet").save(mediasProcessedPath)
     println("预测阶段媒资数据处理完成！")



  }

}
