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

    //val playsProcessedPath="pay_predict/data/train/common/processed/plays"
    val ordersProcessedPath="hdfs:///pay_predict/data/train/common/processed/orders"
    //val mediasProcessedPath="pay_predict/data/train/common/processed/mediastemp"
    //val plays=spark.read.format("parquet").load(playsProcessedPath)
    val orders=spark.read.format("parquet").load(ordersProcessedPath)
    //val medias=spark.read.format("parquet").load(mediasProcessedPath)


    val userProfilePlayPartPath="hdfs:///pay_predict/data/train/common/processed/userprofileplaypart"+args(0)
    val userProfilePreferencePartPath="hdfs:///pay_predict/data/train/common/processed/userprofilepreferencepart"+args(0)
    val userProfileOrderPartPath="hdfs:///pay_predict/data/train/common/processed/userprofileorderpart"+args(0)
    val videoProfilePath="hdfs:///pay_predict/data/train/common/processed/videoprofile"+args(0)
    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
    val videoProfile=spark.read.format("parquet").load(videoProfilePath)



//    //val playsProcessedPath="pay_predict/data/train/common/processed/plays"
//    val ordersProcessedPath="pay_predict/data/train/common/processed/orders"
//    //val mediasProcessedPath="pay_predict/data/train/common/processed/mediastemp"
//    //val plays=spark.read.format("parquet").load(playsProcessedPath)
//    val orders=spark.read.format("parquet").load(ordersProcessedPath)
//    //val medias=spark.read.format("parquet").load(mediasProcessedPath)
//
//
//    val userProfilePlayPartPath="pay_predict/data/train/common/processed/userprofileplaypart"+args(0)
//    val userProfilePreferencePartPath="pay_predict/data/train/common/processed/userprofilepreferencepart"+args(0)
//    val userProfileOrderPartPath="pay_predict/data/train/common/processed/userprofileorderpart"+args(0)
//    val videoProfilePath="pay_predict/data/train/common/processed/videoprofile"+args(0)
//    val userProfilePlayPart = spark.read.format("parquet").load(userProfilePlayPartPath)
//    val userProfilePreferencePart = spark.read.format("parquet").load(userProfilePreferencePartPath)
//    val userProfileOrderPart = spark.read.format("parquet").load(userProfileOrderPartPath)
//    val videoProfile=spark.read.format("parquet").load(videoProfilePath)












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
   // val negativeVideos=temp.select(col(Dic.colVideoId))
    var dataset3=negativeUsers.crossJoin(popularVideo)
    dataset3=dataset3.withColumn(Dic.colOrderStatus,udfAddOrderStatus(col(Dic.colUserId))-1)
    println("第三部分数据条数："+dataset3.count())

    dataset3=dataset3.join(userProfile,joinKeysUserId,"inner")
    dataset3=dataset3.join(videoProfile,joinKeysVideoId,"inner")
    //dataset3.show()
    var result=dataset1.union(dataset2).union(dataset3)

    //result.show()
//    val colList=userProfile.columns.toList
//    val colTypeList=userProfile.dtypes.toList
//    val mapColList=ArrayBuffer[String]()
//    for(elem<- colTypeList){
//      if(!elem._2.equals("StringType") && !elem._2.equals("IntegerType")
//        && !elem._2.equals("DoubleType") && !elem._2.equals("LongType")){
//        mapColList.append(elem._1)
//      }
//    }
//    mapColList.foreach(println)
//    val numColList=colList.diff(mapColList)
//     result=result.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
//      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow))
//     result=result.na.fill(0,numColList)
//
//    val seq=mapColList.toSeq
//    result.select(seq.map(result.col(_)):_*).show()

    val videoFirstCategoryTempPath="hdfs:///pay_predict/data/train/common/processed/videofirstcategorytemp.txt"
    val videoSecondCategoryTempPath="hdfs:///pay_predict/data/train/common/processed/videosecondcategorytemp.txt"
    val labelTempPath="hdfs:///pay_predict/data/train/common/processed/labeltemp.txt"
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
   // println(labelMap)


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
      for(i<- 0 until v1.length; j<- 0 until v2.length;if i==j){
        if(i==j){
          arrayBuffer.append( v1(i)*v2(j) )
        }
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
    def calFirstCategorySimilarity(category:String,preference:Map[String,Int])={
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
    def calSecondCategorySimilarity(category:mutable.WrappedArray[String], preference:Map[String,Int])={
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
    def calLabelSimilarity(category:mutable.WrappedArray[String], preference:Map[String,Int])={
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

    result=result.withColumn("video_level_one_similarity",
      udfCalFirstCategorySimilarity(col(Dic.colVideoOneLevelClassification),col(Dic.colVideoOneLevelPreference)))
      .withColumn("video_level_two_similarity",
        udfCalSecondCategorySimilarity(col(Dic.colVideoTwoLevelClassificationList),col(Dic.colVideoTwoLevelPreference)))
      .withColumn("label_similarity",
      udfCalLabelSimilarity(col(Dic.colVideoTagList),col(Dic.colTagPreference)))
      .withColumn("movie_level_two_similarity",
      udfCalSecondCategorySimilarity(col(Dic.colVideoTwoLevelClassificationList),col(Dic.colMovieTwoLevelPreference)))
      .withColumn("movie_tag_similarity",
      udfCalLabelSimilarity(col(Dic.colVideoTagList),col(Dic.colMovieTagPreference)))
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
    colList-=Dic.colVideoTime
    colList-=Dic.colScore
//    colList.foreach(println)
//    println(colList.length)
    val seqColList=colList.toSeq
    result=result.select(seqColList.map(result.col(_)):_*)
    result=result.na.fill(30,List(Dic.colDaysSinceLastPurchasePackage,Dic.colDaysSinceLastClickPackage,
      Dic.colDaysFromLastActive,Dic.colDaysSinceFirstActiveInTimewindow,Dic.colAbsOfNumberOfDaysBetweenStorageAndCurrent))
    result=result.na.fill(0)
    result.show()
    println("总样本的条数"+result.count())

    val resultSavePath="hdfs:///pay_predict/data/train/singlepoint/ranktraindata"
    result.write.mode(SaveMode.Overwrite).format("parquet").save(resultSavePath+args(0)+"-"+args(2))
    //val csvData=spark.read.format("parquet").load(resultSavePath+args(0)+"-"+args(2))
    //csvData.coalesce(1).write.mode(SaveMode.Overwrite).option("header","true").csv(resultSavePath+args(0)+"-"+args(2)+".csv")
    //result.write.mode(SaveMode.Overwrite).format("tfrecords").option("recordType", "Example").save(resultSavePath+args(0)+"-"+args(2)+".tfrecords")













  }

}
