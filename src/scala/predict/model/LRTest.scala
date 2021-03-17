package predict.model

import mam.GetSaveData.getDataSet
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

import scala.collection.mutable.ArrayBuffer

object LRTest {

  def main(args: Array[String]): Unit = {

    // 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    val trainTime = args(0) + " " + args(1)

    //    val predictTime = args(2) + " " + args(3)

    // 2 數據讀取
    val df_train_set = getDataSet(trainTime, "train")
    printDf("Input df_train_set", df_train_set)

    //    val df_predict_set = getDataSet(predictTime, "predict")


    // 3 数值形与类别形特征处理
//
//    // 选择数值型特征
//    val colType = df_train_set.dtypes.toMap
//    println("Length of train columns: ", df_train_set.columns.length)
//    val NumericTypeString = Array("ByteType", "DecimalType", "DoubleType", "FloatType", "IntegerType", "LongType", "ShortType")
//    val numericCols = ArrayBuffer[String]()
//
//    for ((k, v) <- colType) {
//      if (NumericTypeString.contains(v))
//          numericCols.append(k)
//    }
//
//    println("Numeric type columns length: ", numericCols.length)
//    val trainColsPart = numericCols.filter(!_.contains(Dic.colOrderStatus))
//
//
    // 获得类别型特征的列名
    val col_list = Array(Dic.colVideoOneLevelPreference, Dic.colVideoTwoLevelPreference, Dic.colMovieTwoLevelPreference, Dic.colSingleTwoLevelPreference,
      Dic.colInPackageVideoTwoLevelPreference)

    val cat_cols = ArrayBuffer[String]()
    for (elem <- col_list) {
      for(i <- 1 to 3)
        cat_cols.append(elem + "_" + i)
    }



    /**
     *
     */
    val df = df_train_set.rdd.map(row => LabeledPoint(row.getAs[Double](0), row.getAs[SparseVector](1)))


    // 训练一个GradientBoostedTrees模型
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = 10
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 3
    boostingStrategy.learningRate = 0.3

    // 类别特征
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(df, boostingStrategy)

    print(model)















    //
    //
    //    val lr = new LinearRegression()
    //      .setRegParam(0.01)
    //      .setMaxIter(10000)
    //      .setRegParam(0.3)
    //      .setElasticNetParam(0.8)
    //
    //    val lrModel = lr.fit(df_train_set)
    //
    //
    //
    //    //  转换验证数据集
    //    val validPredicts = lrModel.transform(df_train_set)
    //
    //
    //    // 评估模型性能
    //
    //    val bceval = new BinaryClassificationEvaluator()
    //    print(bceval.evaluate(validPredicts))
    //
    //    // 交叉验证
    //    val cv = new CrossValidator()
    //      .setEstimator(lr)
    //      .setEvaluator(bceval)
    //      .setNumFolds(10)

  }

}
