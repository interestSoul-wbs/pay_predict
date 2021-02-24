package train.userpay

import mam.GetSaveData.hdfsPath
import mam.SparkSessionInit.spark
import mam.Utils.{getCSV, getData, printDf}

object MediasVector {


  def main(args: Array[String]): Unit = {


    // bert

    val df_medias_bert = getCSV(spark, hdfsPath + "data/train/xxkang/medias_bert.csv")

    printDf("df_medias_bert", df_medias_bert)


    //word2vector
    val df_medias_w2v = getData(spark, hdfsPath + "")




    // bert vector concat word2vector





    //PCA De-dimensional




  }

}
