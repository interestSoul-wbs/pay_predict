package train.singlepoint

import mam.Dic
import mam.Utils
import mam.Utils.{calDate, udfAddOrderStatus}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object RankTrainDatasetGenerate {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("RankTrainDatasetGenerate")
      //.master("local[6]")
      .config("spark.sql.crossJoin.enabled","true")  //spark2.x默认不能进行笛卡尔积的操作需要进行设置
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val hdfsPath="hdfs:///pay_predict/"
    //val hdfsPath=""
    val ordersProcessedPath=hdfsPath+"data/train/common/processed/orders"
    val orders=spark.read.format("parquet").load(ordersProcessedPath)

    val userProfilePlayPartPath=hdfsPath+"data/train/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath=hdfsPath+"data/train/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath=hdfsPath+"data/train/common/processed/userprofileorderpart"+args(0)
    val videoProfilePath=hdfsPath+"data/train/common/processed/videoprofile"+args(0)
    val videoVectorPath=hdfsPath+"data/train/common/processed/videovector"+args(0)
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val videoProfile=spark.read.format("parquet").load(videoProfilePath)
    val videoVector=spark.read.format("parquet").load(videoVectorPath)




    val joinKeysUserId=Seq(Dic.colUserId)
    val joinKeysVideoId=Seq(Dic.colVideoId)

    var userProfile=userProfilePlayPart.join(userProfilePreferencePart,joinKeysUserId,"left")
    userProfile=userProfile.join(userProfileOrderPart,joinKeysUserId,"left")


    val predictWindowStart=args(0)+" "+args(1)
    val predictWindowEnd=args(2)+" "+args(3)

    val trainWindowStart=calDate(predictWindowStart,-30 )
    //println(trainWindowStart)


    //在order订单中选出正样本
    val orderSinglePoint=orders
      .filter(
        col(Dic.colResourceType).===(0)
        && col(Dic.colCreationTime).>=(predictWindowStart)
        && col(Dic.colCreationTime).<=(predictWindowEnd)
        && col(Dic.colOrderStatus).>(1)
      ).select(col(Dic.colUserId),col(Dic.colResourceId),col(Dic.colOrderStatus))
      .withColumnRenamed(Dic.colResourceId,Dic.colVideoId)
    var dataset1=orderSinglePoint.join(userProfile,joinKeysUserId,"inner")
    dataset1=dataset1.join(videoProfile,joinKeysVideoId,"inner")
    println("第一部分数据条数："+dataset1.count())
    //temp.show()




    //第一部分的负样本
    val temp=dataset1.select(col(Dic.colUserId),col(Dic.colVideoId))
    //设置负样本中选择多少个video作为负样本中的video
    val negativeN=10
    val popularVideo=temp.groupBy(col(Dic.colVideoId))
      .agg(countDistinct(col(Dic.colUserId)).as("count"))
      .orderBy(col("count").desc)
      .limit(negativeN)
      .select(col(Dic.colVideoId))
    var dataset2=temp.select(col(Dic.colUserId)).distinct().crossJoin(popularVideo).except(temp)
    dataset2=dataset2.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    dataset2=dataset2.join(userProfile,joinKeysUserId,"inner")
    dataset2=dataset2.join(videoProfile,joinKeysVideoId,"inner")
    println("第二部分数据条数："+dataset2.count())
   // dataset2.show()


    //第二部分的负样本
    //开始构造第三部分的样本,用户选自没有在订单中出现过的用户
    val negativeUserN=10*temp.select(col(Dic.colUserId)).distinct().count()
    val negativeUsers=userProfile.select(col(Dic.colUserId)).except(temp.select(col(Dic.colUserId))).limit(negativeUserN.toInt)
    //val negativeVideos=temp.select(col(Dic.colVideoId)).distinct()
    var dataset3=negativeUsers.crossJoin(popularVideo)//.sample(false,1/negativeVideos.count())
    dataset3=dataset3.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    println("第三部分数据条数："+dataset3.count())

    dataset3=dataset3.join(userProfile,joinKeysUserId,"inner")
    dataset3=dataset3.join(videoProfile,joinKeysVideoId,"inner")
    //dataset3.show()
    var result=dataset1.union(dataset2)//.union(dataset3)
    result=result.join(videoVector,joinKeysVideoId,"left")


//    val seq=mapColList.toSeq
//    result.select(seq.map(result.col(_)):_*).show()

    val videoFirstCategoryTempPath=hdfsPath+"data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath=hdfsPath+"data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath=hdfsPath+"data/train/common/processed/labeltemp.txt"
    var videoFirstCategoryMap: Map[String, Int] = Map()
    var videoSecondCategoryMap: Map[String, Int] = Map()
    var labelMap: Map[String, Int] = Map()



    var videoFirstCategory =spark.read.format("csv").load(videoFirstCategoryTempPath)
    var conList=videoFirstCategory.collect()
    for(elem <- conList){
      var s=elem.toString()
      videoFirstCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }
    //println(videoFirstCategoryMap)
    var videoSecondCategory =spark.read.format("csv").load(videoSecondCategoryTempPath)
    conList=videoSecondCategory.collect()
    for(elem <- conList){
      var s=elem.toString()
      videoSecondCategoryMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }
    //println(videoSecondCategoryMap)
    var label=spark.read.format("csv").load(labelTempPath)
    conList=label.collect()
    for(elem <- conList){
      var s=elem.toString()
      labelMap+=(s.substring(1,s.length-1).split("\t")(1) -> s.substring(1,s.length-1).split("\t")(0).toInt)

    }

//    val aMap:Map[String,Int]=Map()
//    for(elem<-mapString.split(",")){
//      aMap+=(elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
//    }
//    //mapString.split(",").foreach(item=>labelMap+=(item.split(" -> ")(0) -> item.split(" -> ")(1).toInt))
//    println(aMap)


    def module(vec:ArrayBuffer[Int]): Double ={
      // math.sqrt( vec.map(x=>x*x).sum )
      math.sqrt(vec.map(math.pow(_,2)).sum)
    }

    /**
     * 求两个向量的内积
     * @param v1
     * @param v2
     */
    def innerProduct(v1:ArrayBuffer[Int],v2:ArrayBuffer[Int]): Double ={
      val arrayBuffer=ArrayBuffer[Double]()
      for(i<- 0 until v1.length-1){
          arrayBuffer.append( v1(i)*v2(i) )
      }
      arrayBuffer.sum
    }

    /**
     * 求两个向量的余弦值
     * @param v1
     * @param v2
     */
    def cosvec(v1:ArrayBuffer[Int],v2:ArrayBuffer[Int]):Double ={
      val cos=innerProduct(v1,v2) / (module(v1)* module(v2))
      if (cos <= 1) cos else 1.0
    }

    def udfCalFirstCategorySimilarity=udf(calFirstCategorySimilarity _)
    def calFirstCategorySimilarity(category:String,preference:Map[String,Int],videoFirstCategoryString:String)={
      var videoFirstCategoryMap: Map[String, Int] = Map()
      for(elem<-videoFirstCategoryString.split(",")){
        videoFirstCategoryMap+=(elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
      }

      if(category==null || preference==null){
        0.0
      }else {
        var categoryArray = new ArrayBuffer[Int](videoFirstCategoryMap.size)
        var preferenceArray = new ArrayBuffer[Int](videoFirstCategoryMap.size)
        for (i <- 0 to videoFirstCategoryMap.size - 1) {
          categoryArray.append(0)
          preferenceArray.append(0)
        }
        for (elem<- preference.keys.toList) {
          var index=videoFirstCategoryMap.getOrElse(elem, 0)
          if(index>=preferenceArray.length) {

          }else{
            preferenceArray(index) = preference.getOrElse(elem, 0)
          }
        }

        var index2=videoFirstCategoryMap.getOrElse(category, 0)
        if(index2>=preferenceArray.length) {

        }else{
          categoryArray(index2) = 1
        }

        cosvec(categoryArray, preferenceArray)
      }

    }
    def udfCalSecondCategorySimilarity=udf(calSecondCategorySimilarity _)
    def calSecondCategorySimilarity(category:mutable.WrappedArray[String], preference:Map[String,Int],videoSecondCategoryString:String)={
      var videoSecondCategoryMap: Map[String, Int] = Map()
      for(elem<-videoSecondCategoryString.split(",")){
        videoSecondCategoryMap+=(elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
      }
      if(category==null || preference==null){
        0.0
      }else {
        var categoryArray = new ArrayBuffer[Int](videoSecondCategoryMap.size)
        var preferenceArray = new ArrayBuffer[Int](videoSecondCategoryMap.size)
        for (i <- 0 to videoSecondCategoryMap.size - 1) {
          categoryArray.append(0)
          preferenceArray.append(0)
        }
        for (elem<- preference.keys.toList) {
          var index=videoSecondCategoryMap.getOrElse(elem, 0)
          if(index>=preferenceArray.length) {

          }else{
            preferenceArray(index) = preference.getOrElse(elem, 0)
          }
        }

        for (elem<- category) {
          var index=videoSecondCategoryMap.getOrElse(elem, 0)
          if(index>=preferenceArray.length) {

          }else{
            categoryArray(index) = 1
          }
        }
        cosvec(categoryArray, preferenceArray)
      }

    }
    def udfCalLabelSimilarity=udf(calLabelSimilarity _)
    def calLabelSimilarity(category:mutable.WrappedArray[String], preference:Map[String,Int],labelString:String)={
      var labelMap: Map[String, Int] = Map()
      for(elem<-labelString.split(",")){
        labelMap+=(elem.split(" -> ")(0) -> elem.split(" -> ")(1).toInt)
      }
      if(category==null || preference==null){
        0.0
      }else {
        var categoryArray = new ArrayBuffer[Int](labelMap.size)
        var preferenceArray = new ArrayBuffer[Int](labelMap.size)
        for (i <- 0 to labelMap.size - 1) {
          categoryArray.append(0)
          preferenceArray.append(0)
        }
        //println(categoryArray.length+" "+preferenceArray.length)
        for (elem<- preference.keys.toList) {
          var index=labelMap.getOrElse(elem, 0)
          if(index>=preferenceArray.length) {

          }else{
            preferenceArray(index) = preference.getOrElse(elem, 0)
          }
        }

        for (elem<- category) {
          var index=labelMap.getOrElse(elem, 0)
          if(index>=preferenceArray.length) {

          }else{
            categoryArray(index) = 1
          }
        }
       // println(categoryArray)
       // println(preferenceArray)
        cosvec(categoryArray, preferenceArray)
      }

    }
//    val testUser=userProfile
//      .filter(
//        !isnull(col(Dic.colVideoOneLevelPreference))
//        && !isnull(col(Dic.colVideoTwoLevelPreference))
//        && !isnull(col(Dic.colTagPreference))
//        //&& !isnull(col(Dic.colMovieTwoLevelPreference))
//        //&& !isnull(col(Dic.colMovieTagPreference))
//      ).limit(5)
//    testUser.show()
//    val testVideo=videoProfile.filter(
//      !isnull(col(Dic.colVideoOneLevelClassification))
//      && !isnull(col(Dic.colVideoTwoLevelClassificationList))
//      && !isnull(col(Dic.colVideoTagList))
//    ).limit(5)
//    testVideo.show()
//
//    val test=testUser.crossJoin(testVideo)

//    result=result.withColumn("video_level_one_similarity",
//      udfCalFirstCategorySimilarity(col(Dic.colVideoOneLevelClassification),
//        col(Dic.colVideoOneLevelPreference),lit(videoFirstCategoryMap.mkString(","))))
//    result=result.withColumn("video_level_two_similarity",
//      udfCalSecondCategorySimilarity(col(Dic.colVideoTwoLevelClassificationList),
//        col(Dic.colVideoTwoLevelPreference),lit(videoSecondCategoryMap.mkString(","))))
//    result=result.withColumn("label_similarity",
//      udfCalLabelSimilarity(col(Dic.colVideoTagList),
//        col(Dic.colTagPreference),lit(labelMap.mkString(","))))
//     result=result.withColumn("movie_level_two_similarity",
//      udfCalSecondCategorySimilarity(col(Dic.colVideoTwoLevelClassificationList),
//        col(Dic.colMovieTwoLevelPreference),lit(videoSecondCategoryMap.mkString(","))))
//    result=result.withColumn("movie_tag_similarity",
//      udfCalLabelSimilarity(col(Dic.colVideoTagList),
//        col(Dic.colMovieTagPreference),lit(labelMap.mkString(","))))
//      .select(col(Dic.colVideoTwoLevelClassificationList),col(Dic.colMovieTwoLevelPreference),col("movie_level_two_similarity"),
//        col(Dic.colVideoTagList),col(Dic.colMovieTagPreference),col("movie_tag_similarity")
//        //col(Dic.colVideoTagList),col(Dic.colTagPreference),col("label_similarity"),
//      )
//      .show(100)


    //数据处理阶段，填补缺失值

   // userProfile.filter(isnull(col(Dic.colMovieTagPreference))).show()


   // videoProfile.columns.toList.foreach(println)



    val colTypeList=result.dtypes.toList
    val colList=ArrayBuffer[String]()
    colList.append(Dic.colUserId)
    colList.append(Dic.colVideoId)
    for(elem<- colTypeList){
      if(elem._2.equals("IntegerType") || elem._2.equals("DoubleType") || elem._2.equals("LongType")){
        colList.append(elem._1)
      }
    }
    colList-=Dic.colIsSingle
    colList-=Dic.colIsTrailers
    colList-=Dic.colIsPaid
    //colList-=Dic.colVideoTime
    //colList-=Dic.colScore
    //colList-=Dic.colPackageId
    //colList.foreach(println)
    //println(colList.length)
    val seqColList=colList.toSeq
    result=result.select(seqColList.map(result.col(_)):_*)
    result=result.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
    result=result.na.fill(0)
    //result.show()
    println("总样本的条数"+result.count())

    val resultSavePath=hdfsPath+"data/train/singlepoint/ranktraindata"
    result.write.mode(SaveMode.Overwrite).format("parquet").save(resultSavePath+args(0)+"-"+args(2))
    val csvData=spark.read.format("parquet").load(resultSavePath+args(0)+"-"+args(2))
    csvData.write.mode(SaveMode.Overwrite).option("header","true").csv(resultSavePath+args(0)+"-"+args(2)+".csv")
    //result.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "Example").save(resultSavePath+args(0)+"-"+args(2)+".tfrecords")













  }

}
