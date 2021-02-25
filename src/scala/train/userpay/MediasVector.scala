package train.userpay

import mam.GetSaveData.{getBertVector, hdfsPath}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf, sysParamSetting}
import org.apache.spark.sql.functions.{col, from_json, when}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StringType}

object MediasVector {


  def main(args: Array[String]): Unit = {

    // 1 SparkSession init
    sysParamSetting
    SparkSessionInit.init()

    // bert

    val df_medias_bert_raw = getBertVector(spark)
    printDf("输入 df_medias_bert_raw", df_medias_bert_raw)

    val df_medias_bert = df_medias_bert_raw
          .select(
            when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
            from_json(col(Dic.colBertVector), ArrayType(StringType, containsNull = true)).as(Dic.colBertVector)

          )


    printDf("df_medias_bert", df_medias_bert)

    df_medias_bert.where(col(Dic.colVideoId) === "261435").show()



    //word2vector
//    val df_medias_w2v = getData(spark, hdfsPath + "")




    // bert vector concat word2vector





    //PCA De-dimensional




  }

}
