package predict.userpay

import mam.GetSaveData.{getBertVector, getProcessedMedias, getVideoFirstCategory, getVideoLabel, hdfsPath, saveDataForXXK}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting, udfGetDays}
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler, Word2Vec}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, explode, from_json, lit, row_number, udf, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType}
import train.userpay.GetMediasVector.{GetPCA, pcaDimension, udfArrayToVec, udfEncodeLabel, vectorDimension, w2vec, windowSize}

object GetMediasVector {


  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    val predictTime = args(0) + " " + args(1)
    println("predictTime", predictTime)

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
      ).na.fill(Map((Dic.colPackageId, -1)))

    // 2-1-1 数值型特征处理
    val df_medias_dig = df_medias_part
      .withColumn(Dic.colReleaseDateGap, udfGetDays(col(Dic.colReleaseDate), lit(predictTime)))
      .withColumn(Dic.colStorageTimeGap, udfGetDays(col(Dic.colStorageTime), lit(predictTime)))

    // 2-1-2 类别型特征编码 一级分类编码
    import scala.collection.mutable
    val videoOneMap = df_video_one.rdd //Dataframe转化为RDD
      .map(row => row.getAs(Dic.colVideoOneLevelClassification).toString -> row.getAs(Dic.colIndex).toString)
      .collectAsMap() //将key-value对类型的RDD转化成Map
      .asInstanceOf[mutable.HashMap[String, String]]

    val df_label_code = df_medias_dig
      .withColumn(Dic.colVideoOneLevelClassification, (udfEncodeLabel(videoOneMap)(col(Dic.colVideoOneLevelClassification))).cast(IntegerType))
      .drop(Dic.colReleaseDate, Dic.colStorageTime)

    printDf("df_label_code", df_label_code)


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


    saveDataForXXK(df_package_code, "common", "medias_digital_category_feature")


    // 数值和类别型特征进行组合
    val assembler = new VectorAssembler()
      .setInputCols(df_package_code.columns.drop(1)) // drop video id
      .setOutputCol(Dic.colDigitalCategoryVec)

    val df_medias_feature = assembler.transform(df_package_code)
      .select(Dic.colVideoId, Dic.colDigitalCategoryVec)

    printDf("df_medias_feature", df_medias_feature)


    // 标准化
    val scaler = new StandardScaler()
      .setInputCol(Dic.colDigitalCategoryVec)
      .setOutputCol(Dic.colDigitalCategoryScalaVec)
      .setWithStd(true) //将数据标准化到单位标准差。
      .setWithMean(true) //是否变换为0均值。
    val df_medias_scalar = scaler
      .fit(df_medias_feature)
      .transform(df_medias_feature)
      .select(Dic.colVideoId, Dic.colDigitalCategoryScalaVec)

    printDf("df_medias_scalar", df_medias_scalar)


    // 2-2 Get bert vector data
    val df_medias_bert_raw = getBertVector("predict")
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


    // 4 Bert vector concat word2vector and medias raw digital cat features

    val df_medias_vec = df_medias_bert
      .join(df_medias_w2v, Seq(Dic.colVideoId), "left")
      .join(df_medias_scalar, Seq(Dic.colVideoId), "left")
      .withColumn(Dic.colBertVector, udfArrayToVec(col(Dic.colBertVector)))


    printDf("df_medias_vec", df_medias_vec)


    val assemblerVideo = new VectorAssembler()
      .setInputCols(Array(Dic.colBertVector, Dic.colVector, Dic.colDigitalCategoryScalaVec))
      .setOutputCol(Dic.colConcatVec)
      .setHandleInvalid("skip")  // Null

    val df_medias_concat = assemblerVideo.transform(df_medias_vec)
      .select(Dic.colVideoId, Dic.colConcatVec)

    printDf("df_medias_concat", df_medias_concat)


    //4 PCA De-dimensional

    val df_medias_pca = GetPCA(df_medias_concat)

    // For nan play history

    val df_fill = spark.createDataFrame(
      Seq(("0", Vectors.dense(Vectors.zeros(pcaDimension).toArray)))
    ).toDF(Dic.colVideoId, Dic.colConcatVec)

    val df_medias_pca_all = df_fill.union(df_medias_pca)

    // 5 Save processed data
    saveDataForXXK(df_medias_pca_all, "predict", "predict_medias_bert_w2v_vec")

    printDf("df_medias_pca_all: PCA De-dimensional concat vector", df_medias_pca_all)


  }

}
