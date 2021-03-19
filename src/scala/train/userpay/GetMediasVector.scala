package train.userpay

import mam.Dic.{colPackageId, colVector}
import mam.GetSaveData.{getBertVector, getProcessedMedias, getVideoFirstCategory, getVideoLabel, hdfsPath, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting, udfGetDays}
import org.apache.spark.ml.feature.{PCA, VectorAssembler, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, collect_list, explode, from_json, lit, row_number, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GetMediasVector {

  // word2vector参数
  val vectorDimension = 256 // 向量维度
  val windowSize = 10 //滑动窗口大小， 默认参数为5，这里尝试设置为10，在一定程度上，windowSize越大，训练越慢,但是向量表达更准确
  val pcaDimension = 64 // 向量维度




  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    val trainTime = args(0) + " " + args(1)
    println("trainTime", trainTime)

    // 2 Get data
    // Medias数据
    val df_medias_processed = getProcessedMedias(spark)
    printDf("输入 df_medias_processed", df_medias_processed)

    val df_video_one = getVideoFirstCategory()
    printDf("输入 df_video_one", df_video_one)

    //2-1 添加数值型特征 , 添加类别型特征

    val df_medias_part = df_medias_processed
      .select(
        Dic.colVideoId, Dic.colScore, Dic.colReleaseDate, Dic.colStorageTime, Dic.colVideoTime,
        Dic.colVideoOneLevelClassification, Dic.colIsPaid, Dic.colPackageId, Dic.colIsSingle, Dic.colIsTrailers
      )

    // 2-1-1 数值型特征处理
    val df_medias_dig = df_medias_part
      .withColumn(Dic.colReleaseDateGap, udfGetDays(col(Dic.colReleaseDate), lit(trainTime)))
      .withColumn(Dic.colStorageTimeGap, udfGetDays(col(Dic.colStorageTime), lit(trainTime)))

    // 2-1-2 类别型特征编码 一级分类编码
    import scala.collection.mutable
    val videoOneMap = df_video_one.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoOneLevelClassification).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    val df_label_code = df_medias_dig
      .withColumn(Dic.colVideoOneLevelClassification, (udfEncodeLabel(videoOneMap)(col(Dic.colVideoOneLevelClassification))).cast(IntegerType))
      .drop(Dic.colReleaseDate, Dic.colStorageTime)

    printDf("df_label_code" , df_label_code)


    // 2-1-3 编码套餐id

    val df_package = df_label_code
      .select(col(Dic.colPackageId))
      .dropDuplicates()
      .withColumn(Dic.colIndex, row_number().over(Window.orderBy(col(Dic.colPackageId))) - 1)

    val packageMap = df_package.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colPackageId).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    val df_package_code = df_label_code
      .withColumn(Dic.colPackageId, (udfEncodeLabel(packageMap)(col(Dic.colPackageId))).cast(IntegerType))

    printDf("df_package_code", df_package_code)


    // 数值和类别型特征进行组合
    val assembler = new VectorAssembler()
      .setInputCols(df_package_code.columns.drop(1)) // drop video id
      .setOutputCol(Dic.colDigitalCategoryVec)

    val df_medias_feature = assembler.transform(df_package_code)
      .select(Dic.colVideoId, Dic.colDigitalCategoryVec)
      .withColumn(Dic.colDigitalCategoryVec, col(Dic.colDigitalCategoryVec).cast(StringType))

    printDf("df_medias_feature", df_medias_feature)



    // 2-2 Get bert vector data
    val df_medias_bert_raw = getBertVector("train")
    printDf("输入 df_medias_bert_raw", df_medias_bert_raw)

    val df_medias_bert = df_medias_bert_raw
      .select(
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        from_json(col(Dic.colBertVector), ArrayType(StringType, containsNull = true)).as(Dic.colBertVector)
      )

    printDf("df_medias_bert", df_medias_bert)

    // 3 word2Vector video id
    // 对videoId进行word2vec
    val df_medias_id = df_medias_bert.agg(collect_list(col(Dic.colVideoId)).as(Dic.colVideoId + "_list"))

    printDf("df_medias_id", df_medias_id)

    val df_medias_w2v = w2vec(df_medias_id)
    printDf("df_medias_w2v", df_medias_w2v)

    val df_medias_ver = df_medias_w2v.withColumn(colVector, col(Dic.colVector).cast(StringType))

    // 4 Bert vector concat word2vector and medias raw digital cat features

    val df_medias_vec = df_medias_bert
      .join(df_medias_ver, Seq(Dic.colVideoId), "left")
      .join(df_medias_feature, Seq(Dic.colVideoId), "left")


    printDf("df_medias_vec", df_medias_vec)

    val df_medias_concat = df_medias_vec.withColumn(Dic.colConcatVec,
      udfConcatVector(col(Dic.colBertVector), col(Dic.colVector), col(Dic.colDigitalCategoryVec)))

    printDf("df_medias_concat: concat bert,videoId w2v and medias raw feature", df_medias_concat)


    //4 PCA De-dimensional

    val df_medias_pca = GetPCA(df_medias_concat)

    // For nan play history

    val df_fill = spark.createDataFrame(
      Seq(("0", Vectors.dense(Vectors.zeros(pcaDimension).toArray)))
    ).toDF(Dic.colVideoId, Dic.colConcatVec)

    val df_medias_pca_all = df_fill.union(df_medias_pca)

    // 5 Save processed data
    saveDataForXXK(df_medias_pca_all, "train", "train_medias_bert_w2v")

    printDf("df_medias_pca_all: PCA De-dimensional concat vector", df_medias_pca_all)


  }

  // word2vec
  def w2vec(df_medias: DataFrame) = {

    val w2vModel = new Word2Vec()
      .setInputCol(Dic.colVideoId + "_list")
      .setOutputCol(Dic.colVector)
      .setVectorSize(vectorDimension)
      .setWindowSize(windowSize)
      .setMinCount(0)
    val model = w2vModel.fit(df_medias)

    model.getVectors
      .withColumnRenamed("word", Dic.colVideoId)
  }


  // concat Bert vector and word2Vec
  def udfConcatVector = udf(concatVector _)

  def concatVector(bert_vector: mutable.WrappedArray[String], w2vector: String, rawMediasFeature: String) = {

    var vectorArray = new ArrayBuffer[Double]()

    bert_vector.foreach(str =>
      vectorArray.append(str.toDouble)
    )

    w2vector.substring(1, w2vector.length - 1).split(",").foreach(
      item =>
        vectorArray.append(item.toDouble)

    )

    rawMediasFeature.substring(1, w2vector.length - 1).split(",").foreach(
      item =>
        vectorArray.append(item.toDouble)
    )

    val v = Vectors.dense(vectorArray.toArray)
    v

  }


  def GetPCA(df: DataFrame) = {
    val pca = new PCA()
      .setInputCol(Dic.colConcatVec)
      .setOutputCol(Dic.colConcatVec + "PCA")
      .setK(pcaDimension)
      .fit(df)


    pca.transform(df)
      .select(Dic.colVideoId, Dic.colConcatVec + "PCA")
      .withColumnRenamed(Dic.colConcatVec + "PCA", Dic.colConcatVec)

  }

  // Encode label one
  def udfEncodeLabel(videoMap: mutable.HashMap[String, String]) = udf((label: String) =>

    videoMap.getOrElse(label, "-1")
  )

}
