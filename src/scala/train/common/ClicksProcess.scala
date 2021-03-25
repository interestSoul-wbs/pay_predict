package train.common

import mam.GetSaveData.{getRawClickData, getRawMediaData, saveLabel, saveProcessedMedia, saveProcessedUserMeta}
import mam.{Dic, SparkSessionInit}
import mam.SparkSessionInit.spark
import mam.Utils.{printDf, sysParamSetting}
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when}
import train.common.MediasProcess.{getArrayStrColLabel, getSingleStrColLabel, mediasProcess}

import scala.collection.mutable

/**
 * @author wj
 * @date 2021/3/17 ${Time}
 * @version 0.1
 * @describe
 */
object ClicksProcess {
  //将字符串属性转化为

  def main(args: Array[String]): Unit = {

    // 1 Spark初始化
    sysParamSetting
    SparkSessionInit.init()

    // 2 數據讀取
    val df_raw_clicks = getRawClickData(spark)
    printDf("输入 df_raw_clicks", df_raw_clicks)

    //     3 对数据进行处理及存儲
    //     3-1
    val df_usermeta_processed = clicksProcess(df_raw_clicks)


    saveProcessedUserMeta(df_usermeta_processed)
    printDf("输出 df_medias_processed", df_usermeta_processed)


    println("ClicksProcess over~~~~~~~~~~~")

  }


  def clicksProcess(df_click: DataFrame) = {


    val df_raw_click = df_click.select(
      when(col(Dic.colUserId) === "NULL", null).otherwise(col(Dic.colUserId)).as(Dic.colUserId),
      when(col(Dic.colDeviceMsg) === "NULL", null).otherwise(col(Dic.colDeviceMsg)).as(Dic.colDeviceMsg),
      when(col(Dic.colFeatureCode) === "NULL", null).otherwise(col(Dic.colFeatureCode)).as(Dic.colFeatureCode),
      when(col(Dic.colBigVersion) === "NULL", null).otherwise(col(Dic.colBigVersion)).as(Dic.colBigVersion),
      when(col(Dic.colProvince) === "NULL", null).otherwise(col(Dic.colProvince)).as(Dic.colProvince),
      when(col(Dic.colCity) === "NULL", null).otherwise(col(Dic.colCity)).as(Dic.colCity),
      when(col(Dic.colCityLevel) === "NULL", null).otherwise(col(Dic.colCityLevel)).as(Dic.colCityLevel),
      when(col(Dic.colAreaId) === "NULL", null).otherwise(col(Dic.colAreaId)).as(Dic.colAreaId)
    )
      .dropDuplicates(Dic.colUserId)
      .na.fill(-1)

    printDf("去重后", df_raw_click)


    var df_raw_click_index = df_raw_click
    var indexModel: StringIndexerModel = null
    val cols = mutable.ListBuffer[String]()
    cols.append(Dic.colUserId)
    for (col <- df_raw_click.columns) {
      if (!col.equals(Dic.colUserId)) {
        cols.append(col + "_index")
        indexModel = new StringIndexer()
          .setInputCol(col)
          .setOutputCol(col + "_index")
          .setHandleInvalid("keep")
          .fit(df_raw_click_index)

        df_raw_click_index = indexModel.transform(df_raw_click_index)
      }
    }

    df_raw_click_index.select(cols.head, cols.tail: _*)
  }


}
