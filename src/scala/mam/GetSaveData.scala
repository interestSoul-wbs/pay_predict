package mam

import mam.SparkSessionInit.spark
import mam.Utils.{getData, printDf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object GetSaveData {

  var tempTable = "temp_table"
    val hdfsPath = ""
  //    val hdfsPath = "/pay_predict_4_Wasu/"
//  val hdfsPath = "/pay_predict_3/"
  val delimiter = ","

  def saveDataForXXK(df_data: DataFrame, state: String, fileName: String) = {
    val path = hdfsPath + "data/" + state + "/xxkang/" + fileName
    saveProcessedData(df_data, path)
  }

  def getDataFromXXK(state: String, fileName: String): DataFrame = {

    val path = hdfsPath + "data/" + state + "/xxkang/" + fileName
    getData(spark, path)

  }


  def saveDataForWx(df_data: DataFrame, state: String, fileName: String) = {

    val path = hdfsPath + "data/" + state + "/wx/" + fileName
    saveProcessedData(df_data, path)

  }

  def getDataFromWx(state: String, fileName: String): DataFrame = {

    val path = hdfsPath + "data/" + state + "/wx/" + fileName
    getData(spark, path)

  }

  def getVideoProfile(spark: SparkSession, now: String, state: String) = {
    val path = ""

    getData(spark, path)
  }


  def getAllBertVector() = {
    val path = hdfsPath + "data/common/xxkang/train_predict_text_bert.csv"

    val schema = StructType(
      List(
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBertVector, StringType)
      )
    )

    val df_bert_raw = spark.read.option("header", false)
      .option("delimiter", "|")
      .schema(schema)
      .format("csv")
      .load(path)
      .select(
        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
        from_json(col(Dic.colBertVector), ArrayType(StringType, containsNull = true)).as(Dic.colBertVector)
      )

    df_bert_raw

  }


  def getOrignalSubId(spark: SparkSession, partitiondate: String, license: String, vod_version: String) = {

    val get_result_sql =
      s"""
         |SELECT
         |    sub_id as subscriberid
         |FROM
         |    vodrs.t_vod_user_appversion_info
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and vod_version='$vod_version'
       """.stripMargin

    val df_sub_id = spark.sql(get_result_sql)
      .dropDuplicates()

    df_sub_id
  }

  /**
   *
   * @param spark
   * @param partitiondate - 这个的选取要最新的
   * @return
   */
  def getOrignalAllUserInfo(spark: SparkSession, partitiondate: String) = {

    val get_result_sql =
      s"""
         |SELECT
         |    subid as subscriberid,
         |    cusid as customerid,
         |    deviceid
         |FROM
         |    vodrs.d_vod_hitvsubscriber
         |WHERE
         |    partitiondate='$partitiondate'
       """.stripMargin

    val df_user_all_info = spark.sql(get_result_sql)
      .dropDuplicates(Array("subscriberid"))

    df_user_all_info
  }

  /**
   * Save data to hive.
   */
  def saveSubIdAndShuntIdInfo(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, vod_version: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_subid_all
        |     (
        |         subscriberid string,
        |         customerid string,
        |         deviceid string,
        |         shunt_subid int,
        |         sector int
        |         )
        |PARTITIONED BY
        |    (partitiondate string, license string, vod_version string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_subid_all
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', vod_version='$vod_version')
         |SELECT
         |    subscriberid,
         |    customerid,
         |    deviceid,
         |    shunt_subid,
         |    sector
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)

    println("vodrs.paypredict_user_subid_all")
    println("over over........... \n" * 4)
  }

  def getAllRawUserInfos(spark: SparkSession, partitiondate: String, license: String, vod_version: String, sector: Int) = {

    val sample_user_order_ori_sql =
      s"""
         |select
         |    subscriberid,
         |    customerid,
         |    deviceid,
         |    sector
         |from
         |    vodrs.paypredict_user_subid_all
         |where
         |    partitiondate='$partitiondate' and license='$license' and vod_version='$vod_version' and sector=$sector
      """.stripMargin

    val df_user_all_info = spark.sql(sample_user_order_ori_sql)

    df_user_all_info
  }

  def getAllRawSubid(spark: SparkSession, partitiondate: String, license: String, vod_version: String, sector: Int) = {

    val sample_user_order_ori_sql =
      s"""
         |select
         |    subscriberid
         |from
         |    vodrs.paypredict_user_subid_all
         |where
         |    partitiondate='$partitiondate' and license='$license' and vod_version='$vod_version' and sector=$sector
      """.stripMargin

    val df_user_all_info = spark.sql(sample_user_order_ori_sql)

    df_user_all_info
  }

  def getRawOrderByDateRangeAllUsers(spark: SparkSession, start_date: String, end_date: String, license: String) = {

    val user_order_ori_sql =
      s"""
         |select
         |    deviceid,
         |    customerid,
         |    fee,
         |    resourcetype,
         |    resourceid,
         |    resourcename,
         |    createdtime,
         |    status,
         |    starttime,
         |    endtime
         |from
         |    vodbasicdim.o_com_vod_all_order
         |where
         |    partitiondate>='$start_date' and partitiondate<='$end_date'  and licence='$license'
      """.stripMargin

    val df_order_all = spark.sql(user_order_ori_sql)
      .withColumn(Dic.colDiscountdesc, lit("No"))
      .select(
        col(Dic.colDeviceid),
        col(Dic.colCustomerid),
        col(Dic.colFee).as(Dic.colMoney),
        col(Dic.colResourcetype).as(Dic.colResourceType),
        col(Dic.colResourceid).as(Dic.colResourceId),
        col(Dic.colResourcename).as(Dic.colResourceTitle),
        col(Dic.colCreatedtime).as(Dic.colCreationTime),
        col(Dic.colDiscountdesc).as(Dic.colDiscountDescription),
        col(Dic.colStatus).as(Dic.colOrderStatus),
        col(Dic.colStarttime).as(Dic.colOrderStartTime),
        col(Dic.colEndtime).as(Dic.colOrderEndTime))

    df_order_all
  }

  /**
   * 从 t_media_sum 获取 物品的数据
   * 2020-11-3 - 目前在测试阶段，在调用该函数时，使用数据分区 partitiondate='20201028' 的数据。
   */
  def getRawMediaData(spark: SparkSession, partitiondate: String, license: String) = {

    val get_result_sql =
      s"""
         |SELECT
         |    media_id as video_id,
         |    title as video_title,
         |    type_name as video_one_level_classification,
         |    category_name_array as video_two_level_classification_list,
         |    tag_name_array as video_tag_list,
         |    director_name_array as director_list,
         |    actor_name_array as actor_list,
         |    country as country,
         |    language as language,
         |    pubdate as release_date,
         |    created_time as storage_time,
         |    cast(time_length as double) video_time,
         |    cast(rate as double) score,
         |    cast(fee as double) is_paid,
         |    cast(vip_id as string) package_id,
         |    cast(is_single as double) is_single,
         |    cast(is_clip as double) is_trailers,
         |    vender_name as supplier,
         |    summary as introduction
         |FROM
         |    vodrs.t_media_sum
         |WHERE
         |    partitiondate = '$partitiondate' and license = '$license'
       """.stripMargin

    val df_medias = spark.sql(get_result_sql)

    df_medias
  }

  /**
   * @author wj
   * @param [spark , rawMediasPath]
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 读取原始文件
   */
  def getRawMediaData(spark: SparkSession) = {

    //
    val mediasRawPath = hdfsPath + "data/train/common/raw/medias/"

    val schema = StructType(
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

    // Konverse - 注意 df 的命名 - df_相关属性 - 不要 dfRawMedia
    val df_raw_media = spark.read
      .option("delimiter", "\\t")
      .option("header", true)
      .schema(schema)
      .csv(mediasRawPath)
    //      .select(
    //        when(col(Dic.colVideoId) === "NULL", null).otherwise(col(Dic.colVideoId)).as(Dic.colVideoId),
    //        when(col(Dic.colVideoTitle) === "NULL", null).otherwise(col(Dic.colVideoTitle)).as(Dic.colVideoTitle),
    //        when(col(Dic.colVideoOneLevelClassification) === "NULL" or (col(Dic.colVideoOneLevelClassification) === ""), null)
    //          .otherwise(col(Dic.colVideoOneLevelClassification)).as(Dic.colVideoOneLevelClassification),
    //        from_json(col(Dic.colVideoTwoLevelClassificationList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTwoLevelClassificationList),
    //        from_json(col(Dic.colVideoTagList), ArrayType(StringType, containsNull = true)).as(Dic.colVideoTagList),
    //        from_json(col(Dic.colDirectorList), ArrayType(StringType, containsNull = true)).as(Dic.colDirectorList),
    //        from_json(col(Dic.colActorList), ArrayType(StringType, containsNull = true)).as(Dic.colActorList),
    //        when(col(Dic.colVideoOneLevelClassification).isNotNull, col(Dic.colVideoOneLevelClassification)), // Konverse - 这一步 相当于缺失值填充，被移动到了 process
    //        when(col(Dic.colCountry) === "NULL", null).otherwise(col(Dic.colCountry)).as(Dic.colCountry),
    //        when(col(Dic.colLanguage) === "NULL", null).otherwise(col(Dic.colLanguage)).as(Dic.colLanguage),
    //        when(col(Dic.colReleaseDate) === "NULL", null).otherwise(col(Dic.colReleaseDate)).as(Dic.colReleaseDate),
    //        when(col(Dic.colStorageTime) === "NULL", null).otherwise(col(Dic.colStorageTime)).as(Dic.colStorageTime), // Konverse - 这一步的udf被移动到了 process
    //        when(col(Dic.colVideoTime) === "NULL", null).otherwise(col(Dic.colVideoTime) cast DoubleType).as(Dic.colVideoTime),
    //        when(col(Dic.colScore) === "NULL", null).otherwise(col(Dic.colScore) cast DoubleType).as(Dic.colScore),
    //        when(col(Dic.colIsPaid) === "NULL", null).otherwise(col(Dic.colIsPaid) cast DoubleType).as(Dic.colIsPaid),
    //        when(col(Dic.colPackageId) === "NULL", null).otherwise(col(Dic.colPackageId)).as(Dic.colPackageId),
    //        when(col(Dic.colIsSingle) === "NULL", null).otherwise(col(Dic.colIsSingle) cast DoubleType).as(Dic.colIsSingle),
    //        when(col(Dic.colIsTrailers) === "NULL", null).otherwise(col(Dic.colIsTrailers) cast DoubleType).as(Dic.colIsTrailers),
    //        when(col(Dic.colSupplier) === "NULL", null).otherwise(col(Dic.colSupplier)).as(Dic.colSupplier),
    //        when(col(Dic.colIntroduction) === "NULL", null).otherwise(col(Dic.colIntroduction)).as(Dic.colIntroduction))

    df_raw_media
  }


  /**
   * Get user order data.
   *
   * @param spark
   * @return
   */
  def getProcessedOrder(spark: SparkSession, partitiondate: String, license: String) = {

    // 1 - 获取用户购买记录
    val user_order_sql =
      s"""
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    vodrs.paypredict_processed_order
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_order = spark.sql(user_order_sql)

    df_order
  }

  def getProcessedOrder(spark: SparkSession, partitiondate: String, license: String, vod_version: String, sector: Int) = {

    // 1 - 获取用户购买记录
    val user_order_sql =
      s"""
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    vodrs.paypredict_processed_order_all
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and vod_version='$vod_version' and sector=$sector
      """.stripMargin

    val df_order = spark.sql(user_order_sql)

    df_order
  }

  def getRawPlayByDateRangeAllUsers(spark: SparkSession, start_date: String, end_date: String, license: String) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    subscriber_id as subscriberid,
         |    time as play_end_time,
         |    item_id as video_id,
         |    sum_duration as broadcast_time
         |FROM
         |    vodrs.t_unified_user_behavior_distinct_day
         |WHERE
         |    partitiondate>='$start_date' and partitiondate<='$end_date' and stage='$license'
         |    and product='vod' and action='play' and event='play_end'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }

  /**
   * 这是已经抽样的用户在一定时间段内的 play 数据，是抽取给山大的数据。
   * 这里的subscriberid 是 rank，要拿到实际的 subscriberid 需要 通过 vodrs.t_vod_user_sample_sdu_v1 join
   *
   * @return
   */
  def getRawPlayByDateRangeSmpleUsers(spark: SparkSession, start_date: String, end_date: String, license: String) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    subscriberid,
         |    time,
         |    itemid,
         |    duration
         |FROM
         |    vodrs.t_sdu_user_play_history_day_sample_users
         |WHERE
         |    partitiondate>='$start_date' and partitiondate<='$end_date' and license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)
      .select(
        col(Dic.colSubscriberid).as(Dic.colUserId),
        col(Dic.colTime).as(Dic.colPlayEndTime),
        col(Dic.colItemid).as(Dic.colVideoId),
        col(Dic.colDuration).as(Dic.colBroadcastTime))

    df_play
  }

  /**
   * Get user play data.
   *
   * @param spark
   * @return
   */
  def getProcessedPlay(spark: SparkSession, partitiondate: String, license: String) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    vodrs.paypredict_processed_play
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }

  def getProcessedPlay(spark: SparkSession, partitiondate: String, license: String, vod_version: String, sector: Int) = {

    // 1 - 获取用户播放记录
    val user_play_sql =
      s"""
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    vodrs.paypredict_processed_play_all
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and vod_version='$vod_version' and sector=$sector
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

    df_play
  }

  /**
   * Get processed user play data.
   *
   * @param spark
   * @return
   */
  def getUserProfilePlayPart(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val data_sql =
      s"""
         |SELECT
         |    user_id,
         |    active_days_last_30_days,
         |    total_time_last_30_days,
         |    days_from_last_active,
         |    days_since_first_active_in_timewindow,
         |    active_days_last_14_days,
         |    total_time_last_14_days,
         |    active_days_last_7_days,
         |    total_time_last_7_days,
         |    active_days_last_3_days,
         |    total_time_last_3_days,
         |    total_time_paid_videos_last_30_days,
         |    total_time_paid_videos_last_14_days,
         |    total_time_paid_videos_last_7_days,
         |    total_time_paid_videos_last_3_days,
         |    total_time_paid_videos_last_1_days,
         |    total_time_in_package_videos_last_30_days,
         |    var_time_in_package_videos_last_30_days,
         |    number_in_package_videos_last_30_days,
         |    total_time_in_package_videos_last_14_days,
         |    var_time_in_package_videos_last_14_days,
         |    number_in_package_videos_last_14_days,
         |    total_time_in_package_videos_last_7_days,
         |    var_time_in_package_videos_last_7_days,
         |    number_in_package_videos_last_7_days,
         |    total_time_in_package_videos_last_3_days,
         |    var_time_in_package_videos_last_3_days,
         |    number_in_package_videos_last_3_days,
         |    total_time_in_package_videos_last_1_days,
         |    var_time_in_package_videos_last_1_days,
         |    number_in_package_videos_last_1_days,
         |    total_time_children_videos_last_30_days,
         |    number_children_videos_last_30_days,
         |    total_time_children_videos_last_14_days,
         |    number_children_videos_last_14_days,
         |    total_time_children_videos_last_7_days,
         |    number_children_videos_last_7_days,
         |    total_time_children_videos_last_3_days,
         |    number_children_videos_last_3_days,
         |    total_time_children_videos_last_1_days,
         |    number_children_videos_last_1_days
         |FROM
         |    vodrs.paypredict_user_profile_play_part
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category'
      """.stripMargin

    val df_user_profile_play = spark.sql(data_sql)

    df_user_profile_play
  }


  def getUserProfilePreferencePart(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val data_sql =
      s"""
         |SELECT
         |     user_id,
         |     total_time_movies_last_30_days,
         |     total_time_movies_last_14_days,
         |     total_time_movies_last_7_days,
         |     total_time_movies_last_3_days,
         |     total_time_movies_last_1_days,
         |     total_time_paid_movies_last_30_days,
         |     total_time_paid_movies_last_14_days,
         |     total_time_paid_movies_last_7_days,
         |     total_time_paid_movies_last_3_days,
         |     total_time_paid_movies_last_1_days,
         |     active_workdays_last_30_days,
         |     avg_workdaily_time_videos_last_30_days,
         |     active_restdays_last_30_days,
         |     avg_restdaily_time_videos_last_30_days,
         |     avg_workdaily_time_paid_videos_last_30_days,
         |     avg_restdaily_time_paid_videos_last_30_days,
         |     video_one_level_preference,
         |     video_two_level_preference,
         |     tag_preference,
         |     movie_two_level_preference,
         |     movie_tag_preference,
         |     single_two_level_preference,
         |     single_tag_preference,
         |     in_package_video_two_level_preference,
         |     in_package_tag_preference
         |FROM
         |    vodrs.paypredict_user_profile_preference_part
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category'
      """.stripMargin

    val df_user_profile_preference = spark.sql(data_sql)

    df_user_profile_preference
  }


  def getUserProfileOrderPart(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val data_sql =
      s"""
         |SELECT
         |    user_id,
         |    number_packages_purchased,
         |    total_money_packages_purchased,
         |    max_money_package_purchased,
         |    min_money_package_purchased,
         |    avg_money_package_purchased,
         |    var_money_package_purchased,
         |    number_singles_purchased,
         |    total_money_singles_purchased,
         |    total_money_consumption,
         |    number_packages_unpurchased,
         |    money_packages_unpurchased,
         |    number_singles_unpurchased,
         |    money_singles_unpurchased,
         |    days_since_last_purchase_package,
         |    days_since_last_click_package,
         |    number_orders_last_30_days,
         |    number_paid_orders_last_30_days,
         |    number_paid_package_last_30_days,
         |    number_paid_single_last_30_days,
         |    days_remaining_package
         |FROM
         |    vodrs.paypredict_user_profile_order_part
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category'
      """.stripMargin

    val df_user_profile_order_part = spark.sql(data_sql)

    df_user_profile_order_part
  }

  def getVideoCategory(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val data_sql =
      s"""
         |SELECT
         |    content
         |FROM
         |    vodrs.paypredict_processed_media_label
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category'
      """.stripMargin

    val df_video_first_category = spark.sql(data_sql)

    df_video_first_category
  }


  def getTrainUser(spark: SparkSession, partitiondate: String, license: String, category: String, new_or_old: String) = {

    val data_sql =
      s"""
         |SELECT
         |    user_id,
         |    order_status
         |FROM
         |    vodrs.paypredict_user_split
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category' and new_or_old='$new_or_old'
      """.stripMargin

    val df_train_user_old = spark.sql(data_sql)

    df_train_user_old
  }

  /**
   * 这是已经抽样的用户在一定时间段内的订单，是抽取给山大的数据。
   * 这里的subscriberid 是 rank，要拿到实际的 subscriberid 需要 通过 vodrs.t_vod_user_sample_sdu_v1 join
   *
   * @return
   */
  def getRawOrderByDateRangeSmpleUsers(spark: SparkSession, start_date: String, end_date: String, license: String) = {

    val sample_user_order_ori_sql =
      s"""
         |select
         |    subscriberid,
         |    fee,
         |    resourcetype,
         |    resourceid,
         |    resourcename,
         |    createdtime,
         |    discountdesc,
         |    status,
         |    starttime,
         |    endtime
         |from
         |    vodrs.t_sdu_user_order_history_day_v1
         |where
         |    partitiondate>='$start_date' and partitiondate<='$end_date'  and license='$license'
      """.stripMargin

    val df_order_ori = spark.sql(sample_user_order_ori_sql)
      .select(
        col(Dic.colSubscriberid).as(Dic.colUserId),
        col(Dic.colFee).as(Dic.colMoney),
        col(Dic.colResourcetype).as(Dic.colResourceType),
        col(Dic.colResourceid).as(Dic.colResourceId),
        col(Dic.colResourcename).as(Dic.colResourceTitle),
        col(Dic.colCreatedtime).as(Dic.colCreationTime),
        col(Dic.colDiscountdesc).as(Dic.colDiscountDescription),
        col(Dic.colStatus).as(Dic.colOrderStatus),
        col(Dic.colStarttime).as(Dic.colOrderStartTime),
        col(Dic.colEndtime).as(Dic.colOrderEndTime))

    df_order_ori
  }

  /**
   * Get smaple users' order data within a period.
   *
   * @param spark
   * @return
   */
  def getRawOrderByDateRange(spark: SparkSession, start_date: String, today: String, license: String) = {

    // 1 - 获取用户购买记录
    val user_order_ori_sql =
      s"""
         |select
         |    userid as subscriberid,
         |    fee,
         |    resourcetype,
         |    resourceid,
         |    createdtime,
         |    discountid,
         |    status,
         |    resourcename,
         |    starttime,
         |    endtime
         |from
         |    vodbasicdim.o_com_vod_all_order
         |where
         |    partitiondate>='$start_date' and partitiondate<='$today' and license='$license'
      """.stripMargin

    val df_order_ori = spark.sql(user_order_ori_sql)

    // 2 - 订单信息
    val order_info_sql =
      s"""
         |SELECT
         |    id as discountid,desc as discountdesc
         |FROM
         |    vodbasicdim.o_vod_ws_discount_info_day
         |WHERE
         |    partitiondate='$today'
      """.stripMargin

    val df_order_info = spark.sql(order_info_sql)

    val df_user_order = df_order_ori.join(df_order_info, Seq(Dic.colDiscountid), "left")

    df_user_order
  }


  /**
   * Get processed media data.
   *
   * @param spark
   * @return
   */
  def getProcessedMedias(spark: SparkSession, partitiondate: String, license: String) = {

    // 1 - get processed medias
    val user_order_sql =
      s"""
         |SELECT
         |    video_id ,
         |    video_title ,
         |    video_one_level_classification ,
         |    video_two_level_classification_list ,
         |    video_tag_list ,
         |    director_list ,
         |    actor_list ,
         |    country ,
         |    language ,
         |    release_date ,
         |    storage_time ,
         |    video_time ,
         |    score ,
         |    is_paid ,
         |    package_id ,
         |    is_single ,
         |    is_trailers ,
         |    supplier ,
         |    introduction
         |FROM
         |    vodrs.paypredict_processed_media
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_medias = spark.sql(user_order_sql)

    df_medias
  }


  def getVideoProfile(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val exec_sql =
      s"""
         |SELECT
         |    video_id,
         |    video_title,
         |    video_one_level_classification,
         |    video_two_level_classification_list,
         |    video_tag_list,
         |    director_list,
         |    actor_list,
         |    country,
         |    language,
         |    release_date,
         |    storage_time,
         |    video_time,
         |    score,
         |    is_paid,
         |    package_id,
         |    is_single,
         |    is_trailers,
         |    supplier,
         |    introduction,
         |    number_of_plays_in_30_days,
         |    number_of_views_within_30_days,
         |    number_of_plays_in_14_days,
         |    number_of_views_within_14_days,
         |    number_of_plays_in_7_days,
         |    number_of_views_within_7_days,
         |    number_of_plays_in_3_days,
         |    number_of_views_within_3_days,
         |    abs_of_number_of_days_between_storage_and_current,
         |    number_of_times_purchased_within_30_days,
         |    number_of_times_purchased_within_14_days,
         |    number_of_times_purchased_within_7_days,
         |    number_of_times_purchased_within_3_days,
         |    number_of_times_purchased_total
         |FROM
         |    vodrs.paypredict_user_video_profile
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and category='$category'
      """.stripMargin

    val df_video_profile = spark.sql(exec_sql)

    df_video_profile
  }

  def getVideoVector(spark: SparkSession, partitiondate: String, license: String) = {

    val exec_sql =
      s"""
         |SELECT
         |    video_id,
         |    v_0,
         |    v_1,
         |    v_2,
         |    v_3,
         |    v_4,
         |    v_5,
         |    v_6,
         |    v_7,
         |    v_8,
         |    v_9,
         |    v_10,
         |    v_11,
         |    v_12,
         |    v_13,
         |    v_14,
         |    v_15,
         |    v_16,
         |    v_17,
         |    v_18,
         |    v_19,
         |    v_20,
         |    v_21,
         |    v_22,
         |    v_23,
         |    v_24,
         |    v_25,
         |    v_26,
         |    v_27,
         |    v_28,
         |    v_29,
         |    v_30,
         |    v_31,
         |    v_32,
         |    v_33,
         |    v_34,
         |    v_35,
         |    v_36,
         |    v_37,
         |    v_38,
         |    v_39,
         |    v_40,
         |    v_41,
         |    v_42,
         |    v_43,
         |    v_44,
         |    v_45,
         |    v_46,
         |    v_47,
         |    v_48,
         |    v_49,
         |    v_50,
         |    v_51,
         |    v_52,
         |    v_53,
         |    v_54,
         |    v_55,
         |    v_56,
         |    v_57,
         |    v_58,
         |    v_59,
         |    v_60,
         |    v_61,
         |    v_62,
         |    v_63
         |FROM
         |    vodrs.paypredict_video_vector
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_video_vector = spark.sql(exec_sql)

    df_video_vector
  }


  def getUserDivisionResult(spark: SparkSession, partitiondate: String, license: String, category: String) = {

    val get_result_sql =
      s"""
         |SELECT
         |    *
         |FROM
         |    vodrs.paypredict_singlepoint_user_division
         |WHERE
         |    partitiondate = '$partitiondate' and license = '$license' and category='$category'
       """.stripMargin

    val df_user_division = spark.sql(get_result_sql)
      .drop(Dic.colPartitionDate, Dic.colLicense, Dic.colCategory)

    df_user_division
  }


  def saveSinglepointRankData(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_singlepoint_rank(
        |        user_id string,
        |        video_id string,
        |        order_status double,
        |        active_days_last_30_days double,
        |        total_time_last_30_days double,
        |        days_from_last_active double,
        |        days_since_first_active_in_timewindow double,
        |        active_days_last_14_days double,
        |        total_time_last_14_days double,
        |        active_days_last_7_days double,
        |        total_time_last_7_days double,
        |        active_days_last_3_days double,
        |        total_time_last_3_days double,
        |        total_time_paid_videos_last_30_days double,
        |        total_time_paid_videos_last_14_days double,
        |        total_time_paid_videos_last_7_days double,
        |        total_time_paid_videos_last_3_days double,
        |        total_time_paid_videos_last_1_days double,
        |        total_time_in_package_videos_last_30_days double,
        |        var_time_in_package_videos_last_30_days double,
        |        number_in_package_videos_last_30_days double,
        |        total_time_in_package_videos_last_14_days double,
        |        var_time_in_package_videos_last_14_days double,
        |        number_in_package_videos_last_14_days double,
        |        total_time_in_package_videos_last_7_days double,
        |        var_time_in_package_videos_last_7_days double,
        |        number_in_package_videos_last_7_days double,
        |        total_time_in_package_videos_last_3_days double,
        |        var_time_in_package_videos_last_3_days double,
        |        number_in_package_videos_last_3_days double,
        |        total_time_in_package_videos_last_1_days double,
        |        var_time_in_package_videos_last_1_days double,
        |        number_in_package_videos_last_1_days double,
        |        total_time_children_videos_last_30_days double,
        |        number_children_videos_last_30_days double,
        |        total_time_children_videos_last_14_days double,
        |        number_children_videos_last_14_days double,
        |        total_time_children_videos_last_7_days double,
        |        number_children_videos_last_7_days double,
        |        total_time_children_videos_last_3_days double,
        |        number_children_videos_last_3_days double,
        |        total_time_children_videos_last_1_days double,
        |        number_children_videos_last_1_days double,
        |        total_time_movies_last_30_days double,
        |        total_time_movies_last_14_days double,
        |        total_time_movies_last_7_days double,
        |        total_time_movies_last_3_days double,
        |        total_time_movies_last_1_days double,
        |        total_time_paid_movies_last_30_days double,
        |        total_time_paid_movies_last_14_days double,
        |        total_time_paid_movies_last_7_days double,
        |        total_time_paid_movies_last_3_days double,
        |        total_time_paid_movies_last_1_days double,
        |        active_workdays_last_30_days double,
        |        avg_workdaily_time_videos_last_30_days double,
        |        active_restdays_last_30_days double,
        |        avg_restdaily_time_videos_last_30_days double,
        |        avg_workdaily_time_paid_videos_last_30_days double,
        |        avg_restdaily_time_paid_videos_last_30_days double,
        |        number_packages_purchased double,
        |        total_money_packages_purchased double,
        |        max_money_package_purchased double,
        |        min_money_package_purchased double,
        |        avg_money_package_purchased double,
        |        var_money_package_purchased double,
        |        number_singles_purchased double,
        |        total_money_singles_purchased double,
        |        total_money_consumption double,
        |        number_packages_unpurchased double,
        |        money_packages_unpurchased double,
        |        number_singles_unpurchased double,
        |        money_singles_unpurchased double,
        |        days_since_last_purchase_package double,
        |        days_since_last_click_package double,
        |        number_orders_last_30_days double,
        |        number_paid_orders_last_30_days double,
        |        number_paid_package_last_30_days double,
        |        number_paid_single_last_30_days double,
        |        days_remaining_package double,
        |        video_time double,
        |        score double,
        |        number_of_plays_in_30_days double,
        |        number_of_views_within_30_days double,
        |        number_of_plays_in_14_days double,
        |        number_of_views_within_14_days double,
        |        number_of_plays_in_7_days double,
        |        number_of_views_within_7_days double,
        |        number_of_plays_in_3_days double,
        |        number_of_views_within_3_days double,
        |        abs_of_number_of_days_between_storage_and_current double,
        |        number_of_times_purchased_within_30_days double,
        |        number_of_times_purchased_within_14_days double,
        |        number_of_times_purchased_within_7_days double,
        |        number_of_times_purchased_within_3_days double,
        |        number_of_times_purchased_total double,
        |        v_0 double,
        |        v_1 double,
        |        v_2 double,
        |        v_3 double,
        |        v_4 double,
        |        v_5 double,
        |        v_6 double,
        |        v_7 double,
        |        v_8 double,
        |        v_9 double,
        |        v_10 double,
        |        v_11 double,
        |        v_12 double,
        |        v_13 double,
        |        v_14 double,
        |        v_15 double,
        |        v_16 double,
        |        v_17 double,
        |        v_18 double,
        |        v_19 double,
        |        v_20 double,
        |        v_21 double,
        |        v_22 double,
        |        v_23 double,
        |        v_24 double,
        |        v_25 double,
        |        v_26 double,
        |        v_27 double,
        |        v_28 double,
        |        v_29 double,
        |        v_30 double,
        |        v_31 double,
        |        v_32 double,
        |        v_33 double,
        |        v_34 double,
        |        v_35 double,
        |        v_36 double,
        |        v_37 double,
        |        v_38 double,
        |        v_39 double,
        |        v_40 double,
        |        v_41 double,
        |        v_42 double,
        |        v_43 double,
        |        v_44 double,
        |        v_45 double,
        |        v_46 double,
        |        v_47 double,
        |        v_48 double,
        |        v_49 double,
        |        v_50 double,
        |        v_51 double,
        |        v_52 double,
        |        v_53 double,
        |        v_54 double,
        |        v_55 double,
        |        v_56 double,
        |        v_57 double,
        |        v_58 double,
        |        v_59 double,
        |        v_60 double,
        |        v_61 double,
        |        v_62 double,
        |        v_63 double)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_singlepoint_rank
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |        user_id,
         |        video_id,
         |        order_status,
         |        active_days_last_30_days,
         |        total_time_last_30_days,
         |        days_from_last_active,
         |        days_since_first_active_in_timewindow,
         |        active_days_last_14_days,
         |        total_time_last_14_days,
         |        active_days_last_7_days,
         |        total_time_last_7_days,
         |        active_days_last_3_days,
         |        total_time_last_3_days,
         |        total_time_paid_videos_last_30_days,
         |        total_time_paid_videos_last_14_days,
         |        total_time_paid_videos_last_7_days,
         |        total_time_paid_videos_last_3_days,
         |        total_time_paid_videos_last_1_days,
         |        total_time_in_package_videos_last_30_days,
         |        var_time_in_package_videos_last_30_days,
         |        number_in_package_videos_last_30_days,
         |        total_time_in_package_videos_last_14_days,
         |        var_time_in_package_videos_last_14_days,
         |        number_in_package_videos_last_14_days,
         |        total_time_in_package_videos_last_7_days,
         |        var_time_in_package_videos_last_7_days,
         |        number_in_package_videos_last_7_days,
         |        total_time_in_package_videos_last_3_days,
         |        var_time_in_package_videos_last_3_days,
         |        number_in_package_videos_last_3_days,
         |        total_time_in_package_videos_last_1_days,
         |        var_time_in_package_videos_last_1_days,
         |        number_in_package_videos_last_1_days,
         |        total_time_children_videos_last_30_days,
         |        number_children_videos_last_30_days,
         |        total_time_children_videos_last_14_days,
         |        number_children_videos_last_14_days,
         |        total_time_children_videos_last_7_days,
         |        number_children_videos_last_7_days,
         |        total_time_children_videos_last_3_days,
         |        number_children_videos_last_3_days,
         |        total_time_children_videos_last_1_days,
         |        number_children_videos_last_1_days,
         |        total_time_movies_last_30_days,
         |        total_time_movies_last_14_days,
         |        total_time_movies_last_7_days,
         |        total_time_movies_last_3_days,
         |        total_time_movies_last_1_days,
         |        total_time_paid_movies_last_30_days,
         |        total_time_paid_movies_last_14_days,
         |        total_time_paid_movies_last_7_days,
         |        total_time_paid_movies_last_3_days,
         |        total_time_paid_movies_last_1_days,
         |        active_workdays_last_30_days,
         |        avg_workdaily_time_videos_last_30_days,
         |        active_restdays_last_30_days,
         |        avg_restdaily_time_videos_last_30_days,
         |        avg_workdaily_time_paid_videos_last_30_days,
         |        avg_restdaily_time_paid_videos_last_30_days,
         |        number_packages_purchased,
         |        total_money_packages_purchased,
         |        max_money_package_purchased,
         |        min_money_package_purchased,
         |        avg_money_package_purchased,
         |        var_money_package_purchased,
         |        number_singles_purchased,
         |        total_money_singles_purchased,
         |        total_money_consumption,
         |        number_packages_unpurchased,
         |        money_packages_unpurchased,
         |        number_singles_unpurchased,
         |        money_singles_unpurchased,
         |        days_since_last_purchase_package,
         |        days_since_last_click_package,
         |        number_orders_last_30_days,
         |        number_paid_orders_last_30_days,
         |        number_paid_package_last_30_days,
         |        number_paid_single_last_30_days,
         |        days_remaining_package,
         |        video_time,
         |        score,
         |        number_of_plays_in_30_days,
         |        number_of_views_within_30_days,
         |        number_of_plays_in_14_days,
         |        number_of_views_within_14_days,
         |        number_of_plays_in_7_days,
         |        number_of_views_within_7_days,
         |        number_of_plays_in_3_days,
         |        number_of_views_within_3_days,
         |        abs_of_number_of_days_between_storage_and_current,
         |        number_of_times_purchased_within_30_days,
         |        number_of_times_purchased_within_14_days,
         |        number_of_times_purchased_within_7_days,
         |        number_of_times_purchased_within_3_days,
         |        number_of_times_purchased_total,
         |        v_0,
         |        v_1,
         |        v_2,
         |        v_3,
         |        v_4,
         |        v_5,
         |        v_6,
         |        v_7,
         |        v_8,
         |        v_9,
         |        v_10,
         |        v_11,
         |        v_12,
         |        v_13,
         |        v_14,
         |        v_15,
         |        v_16,
         |        v_17,
         |        v_18,
         |        v_19,
         |        v_20,
         |        v_21,
         |        v_22,
         |        v_23,
         |        v_24,
         |        v_25,
         |        v_26,
         |        v_27,
         |        v_28,
         |        v_29,
         |        v_30,
         |        v_31,
         |        v_32,
         |        v_33,
         |        v_34,
         |        v_35,
         |        v_36,
         |        v_37,
         |        v_38,
         |        v_39,
         |        v_40,
         |        v_41,
         |        v_42,
         |        v_43,
         |        v_44,
         |        v_45,
         |        v_46,
         |        v_47,
         |        v_48,
         |        v_49,
         |        v_50,
         |        v_51,
         |        v_52,
         |        v_53,
         |        v_54,
         |        v_55,
         |        v_56,
         |        v_57,
         |        v_58,
         |        v_59,
         |        v_60,
         |        v_61,
         |        v_62,
         |        v_63
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  def saveSinglepointUserDivisionData(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_singlepoint_user_division(
        |            user_id string,
        |            active_days_last_30_days double,
        |            total_time_last_30_days double,
        |            days_from_last_active double,
        |            days_since_first_active_in_timewindow double,
        |            active_days_last_14_days double,
        |            total_time_last_14_days double,
        |            active_days_last_7_days double,
        |            total_time_last_7_days double,
        |            active_days_last_3_days double,
        |            total_time_last_3_days double,
        |            total_time_paid_videos_last_30_days double,
        |            total_time_paid_videos_last_14_days double,
        |            total_time_paid_videos_last_7_days double,
        |            total_time_paid_videos_last_3_days double,
        |            total_time_paid_videos_last_1_days double,
        |            total_time_in_package_videos_last_30_days double,
        |            var_time_in_package_videos_last_30_days double,
        |            number_in_package_videos_last_30_days double,
        |            total_time_in_package_videos_last_14_days double,
        |            var_time_in_package_videos_last_14_days double,
        |            number_in_package_videos_last_14_days double,
        |            total_time_in_package_videos_last_7_days double,
        |            var_time_in_package_videos_last_7_days double,
        |            number_in_package_videos_last_7_days double,
        |            total_time_in_package_videos_last_3_days double,
        |            var_time_in_package_videos_last_3_days double,
        |            number_in_package_videos_last_3_days double,
        |            total_time_in_package_videos_last_1_days double,
        |            var_time_in_package_videos_last_1_days double,
        |            number_in_package_videos_last_1_days double,
        |            total_time_children_videos_last_30_days double,
        |            number_children_videos_last_30_days double,
        |            total_time_children_videos_last_14_days double,
        |            number_children_videos_last_14_days double,
        |            total_time_children_videos_last_7_days double,
        |            number_children_videos_last_7_days double,
        |            total_time_children_videos_last_3_days double,
        |            number_children_videos_last_3_days double,
        |            total_time_children_videos_last_1_days double,
        |            number_children_videos_last_1_days double,
        |            total_time_movies_last_30_days double,
        |            total_time_movies_last_14_days double,
        |            total_time_movies_last_7_days double,
        |            total_time_movies_last_3_days double,
        |            total_time_movies_last_1_days double,
        |            total_time_paid_movies_last_30_days double,
        |            total_time_paid_movies_last_14_days double,
        |            total_time_paid_movies_last_7_days double,
        |            total_time_paid_movies_last_3_days double,
        |            total_time_paid_movies_last_1_days double,
        |            active_workdays_last_30_days double,
        |            avg_workdaily_time_videos_last_30_days double,
        |            active_restdays_last_30_days double,
        |            avg_restdaily_time_videos_last_30_days double,
        |            avg_workdaily_time_paid_videos_last_30_days double,
        |            avg_restdaily_time_paid_videos_last_30_days double,
        |            number_packages_purchased double,
        |            total_money_packages_purchased double,
        |            max_money_package_purchased double,
        |            min_money_package_purchased double,
        |            avg_money_package_purchased double,
        |            var_money_package_purchased double,
        |            number_singles_purchased double,
        |            total_money_singles_purchased double,
        |            total_money_consumption double,
        |            number_packages_unpurchased double,
        |            money_packages_unpurchased double,
        |            number_singles_unpurchased double,
        |            money_singles_unpurchased double,
        |            days_since_last_purchase_package double,
        |            days_since_last_click_package double,
        |            number_orders_last_30_days double,
        |            number_paid_orders_last_30_days double,
        |            number_paid_package_last_30_days double,
        |            number_paid_single_last_30_days double,
        |            days_remaining_package double,
        |            order_status double)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_singlepoint_user_division
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |        user_id,
         |        active_days_last_30_days,
         |        total_time_last_30_days,
         |        days_from_last_active,
         |        days_since_first_active_in_timewindow,
         |        active_days_last_14_days,
         |        total_time_last_14_days,
         |        active_days_last_7_days,
         |        total_time_last_7_days,
         |        active_days_last_3_days,
         |        total_time_last_3_days,
         |        total_time_paid_videos_last_30_days,
         |        total_time_paid_videos_last_14_days,
         |        total_time_paid_videos_last_7_days,
         |        total_time_paid_videos_last_3_days,
         |        total_time_paid_videos_last_1_days,
         |        total_time_in_package_videos_last_30_days,
         |        var_time_in_package_videos_last_30_days,
         |        number_in_package_videos_last_30_days,
         |        total_time_in_package_videos_last_14_days,
         |        var_time_in_package_videos_last_14_days,
         |        number_in_package_videos_last_14_days,
         |        total_time_in_package_videos_last_7_days,
         |        var_time_in_package_videos_last_7_days,
         |        number_in_package_videos_last_7_days,
         |        total_time_in_package_videos_last_3_days,
         |        var_time_in_package_videos_last_3_days,
         |        number_in_package_videos_last_3_days,
         |        total_time_in_package_videos_last_1_days,
         |        var_time_in_package_videos_last_1_days,
         |        number_in_package_videos_last_1_days,
         |        total_time_children_videos_last_30_days,
         |        number_children_videos_last_30_days,
         |        total_time_children_videos_last_14_days,
         |        number_children_videos_last_14_days,
         |        total_time_children_videos_last_7_days,
         |        number_children_videos_last_7_days,
         |        total_time_children_videos_last_3_days,
         |        number_children_videos_last_3_days,
         |        total_time_children_videos_last_1_days,
         |        number_children_videos_last_1_days,
         |        total_time_movies_last_30_days,
         |        total_time_movies_last_14_days,
         |        total_time_movies_last_7_days,
         |        total_time_movies_last_3_days,
         |        total_time_movies_last_1_days,
         |        total_time_paid_movies_last_30_days,
         |        total_time_paid_movies_last_14_days,
         |        total_time_paid_movies_last_7_days,
         |        total_time_paid_movies_last_3_days,
         |        total_time_paid_movies_last_1_days,
         |        active_workdays_last_30_days,
         |        avg_workdaily_time_videos_last_30_days,
         |        active_restdays_last_30_days,
         |        avg_restdaily_time_videos_last_30_days,
         |        avg_workdaily_time_paid_videos_last_30_days,
         |        avg_restdaily_time_paid_videos_last_30_days,
         |        number_packages_purchased,
         |        total_money_packages_purchased,
         |        max_money_package_purchased,
         |        min_money_package_purchased,
         |        avg_money_package_purchased,
         |        var_money_package_purchased,
         |        number_singles_purchased,
         |        total_money_singles_purchased,
         |        total_money_consumption,
         |        number_packages_unpurchased,
         |        money_packages_unpurchased,
         |        number_singles_unpurchased,
         |        money_singles_unpurchased,
         |        days_since_last_purchase_package,
         |        days_since_last_click_package,
         |        number_orders_last_30_days,
         |        number_paid_orders_last_30_days,
         |        number_paid_package_last_30_days,
         |        number_paid_single_last_30_days,
         |        days_remaining_package,
         |        order_status
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  def saveUserSplitResult(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String,
                          new_or_old: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_split(
        |            user_id string,
        |            order_status int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string, new_or_old string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_split
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category', new_or_old='$new_or_old')
         |SELECT
         |    user_id,
         |    order_status
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  def saveFeatureProcessResult(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String, new_or_old: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_feature_process(
        |            user_id string,
        |            order_status int,
        |            active_days_last_30_days long,
        |            total_time_last_30_days double,
        |            days_from_last_active int,
        |            days_since_first_active_in_timewindow int,
        |            active_days_last_14_days long,
        |            total_time_last_14_days double,
        |            active_days_last_7_days long,
        |            total_time_last_7_days double,
        |            active_days_last_3_days long,
        |            total_time_last_3_days double,
        |            total_time_paid_videos_last_30_days double,
        |            total_time_paid_videos_last_14_days double,
        |            total_time_paid_videos_last_7_days double,
        |            total_time_paid_videos_last_3_days double,
        |            total_time_paid_videos_last_1_days double,
        |            total_time_in_package_videos_last_30_days double,
        |            var_time_in_package_videos_last_30_days double,
        |            number_in_package_videos_last_30_days long,
        |            total_time_in_package_videos_last_14_days double,
        |            var_time_in_package_videos_last_14_days double,
        |            number_in_package_videos_last_14_days long,
        |            total_time_in_package_videos_last_7_days double,
        |            var_time_in_package_videos_last_7_days double,
        |            number_in_package_videos_last_7_days long,
        |            total_time_in_package_videos_last_3_days double,
        |            var_time_in_package_videos_last_3_days double,
        |            number_in_package_videos_last_3_days long,
        |            total_time_in_package_videos_last_1_days double,
        |            var_time_in_package_videos_last_1_days double,
        |            number_in_package_videos_last_1_days long,
        |            total_time_children_videos_last_30_days double,
        |            number_children_videos_last_30_days long,
        |            total_time_children_videos_last_14_days double,
        |            number_children_videos_last_14_days long,
        |            total_time_children_videos_last_7_days double,
        |            number_children_videos_last_7_days long,
        |            total_time_children_videos_last_3_days double,
        |            number_children_videos_last_3_days long,
        |            total_time_children_videos_last_1_days double,
        |            number_children_videos_last_1_days long,
        |            total_time_movies_last_30_days double,
        |            total_time_movies_last_14_days double,
        |            total_time_movies_last_7_days double,
        |            total_time_movies_last_3_days double,
        |            total_time_movies_last_1_days double,
        |            total_time_paid_movies_last_30_days double,
        |            total_time_paid_movies_last_14_days double,
        |            total_time_paid_movies_last_7_days double,
        |            total_time_paid_movies_last_3_days double,
        |            total_time_paid_movies_last_1_days double,
        |            active_workdays_last_30_days long,
        |            avg_workdaily_time_videos_last_30_days double,
        |            active_restdays_last_30_days long,
        |            avg_restdaily_time_videos_last_30_days double,
        |            avg_workdaily_time_paid_videos_last_30_days double,
        |            avg_restdaily_time_paid_videos_last_30_days double,
        |            number_packages_purchased long,
        |            total_money_packages_purchased double,
        |            max_money_package_purchased double,
        |            min_money_package_purchased double,
        |            avg_money_package_purchased double,
        |            var_money_package_purchased double,
        |            number_singles_purchased long,
        |            total_money_singles_purchased double,
        |            total_money_consumption double,
        |            number_packages_unpurchased long,
        |            money_packages_unpurchased double,
        |            number_singles_unpurchased long,
        |            money_singles_unpurchased double,
        |            days_since_last_purchase_package int,
        |            days_since_last_click_package int,
        |            number_orders_last_30_days long,
        |            number_paid_orders_last_30_days long,
        |            number_paid_package_last_30_days long,
        |            number_paid_single_last_30_days long,
        |            days_remaining_package int,
        |            video_one_level_preference_1 int,
        |            video_one_level_preference_2 int,
        |            video_one_level_preference_3 int,
        |            video_two_level_preference_1 int,
        |            video_two_level_preference_2 int,
        |            video_two_level_preference_3 int,
        |            movie_two_level_preference_1 int,
        |            movie_two_level_preference_2 int,
        |            movie_two_level_preference_3 int,
        |            single_two_level_preference_1 int,
        |            single_two_level_preference_2 int,
        |            single_two_level_preference_3 int,
        |            in_package_video_two_level_preference_1 int,
        |            in_package_video_two_level_preference_2 int,
        |            in_package_video_two_level_preference_3 int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string, new_or_old string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_feature_process
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category', new_or_old='$new_or_old')
         |SELECT
         |    user_id,
         |    order_status,
         |    active_days_last_30_days,
         |    total_time_last_30_days,
         |    days_from_last_active,
         |    days_since_first_active_in_timewindow,
         |    active_days_last_14_days,
         |    total_time_last_14_days,
         |    active_days_last_7_days,
         |    total_time_last_7_days,
         |    active_days_last_3_days,
         |    total_time_last_3_days,
         |    total_time_paid_videos_last_30_days,
         |    total_time_paid_videos_last_14_days,
         |    total_time_paid_videos_last_7_days,
         |    total_time_paid_videos_last_3_days,
         |    total_time_paid_videos_last_1_days,
         |    total_time_in_package_videos_last_30_days,
         |    var_time_in_package_videos_last_30_days,
         |    number_in_package_videos_last_30_days,
         |    total_time_in_package_videos_last_14_days,
         |    var_time_in_package_videos_last_14_days,
         |    number_in_package_videos_last_14_days,
         |    total_time_in_package_videos_last_7_days,
         |    var_time_in_package_videos_last_7_days,
         |    number_in_package_videos_last_7_days,
         |    total_time_in_package_videos_last_3_days,
         |    var_time_in_package_videos_last_3_days,
         |    number_in_package_videos_last_3_days,
         |    total_time_in_package_videos_last_1_days,
         |    var_time_in_package_videos_last_1_days,
         |    number_in_package_videos_last_1_days,
         |    total_time_children_videos_last_30_days,
         |    number_children_videos_last_30_days,
         |    total_time_children_videos_last_14_days,
         |    number_children_videos_last_14_days,
         |    total_time_children_videos_last_7_days,
         |    number_children_videos_last_7_days,
         |    total_time_children_videos_last_3_days,
         |    number_children_videos_last_3_days,
         |    total_time_children_videos_last_1_days,
         |    number_children_videos_last_1_days,
         |    total_time_movies_last_30_days,
         |    total_time_movies_last_14_days,
         |    total_time_movies_last_7_days,
         |    total_time_movies_last_3_days,
         |    total_time_movies_last_1_days,
         |    total_time_paid_movies_last_30_days,
         |    total_time_paid_movies_last_14_days,
         |    total_time_paid_movies_last_7_days,
         |    total_time_paid_movies_last_3_days,
         |    total_time_paid_movies_last_1_days,
         |    active_workdays_last_30_days,
         |    avg_workdaily_time_videos_last_30_days,
         |    active_restdays_last_30_days,
         |    avg_restdaily_time_videos_last_30_days,
         |    avg_workdaily_time_paid_videos_last_30_days,
         |    avg_restdaily_time_paid_videos_last_30_days,
         |    number_packages_purchased,
         |    total_money_packages_purchased,
         |    max_money_package_purchased,
         |    min_money_package_purchased,
         |    avg_money_package_purchased,
         |    var_money_package_purchased,
         |    number_singles_purchased,
         |    total_money_singles_purchased,
         |    total_money_consumption,
         |    number_packages_unpurchased,
         |    money_packages_unpurchased,
         |    number_singles_unpurchased,
         |    money_singles_unpurchased,
         |    days_since_last_purchase_package,
         |    days_since_last_click_package,
         |    number_orders_last_30_days,
         |    number_paid_orders_last_30_days,
         |    number_paid_package_last_30_days,
         |    number_paid_single_last_30_days,
         |    days_remaining_package,
         |    video_one_level_preference_1,
         |    video_one_level_preference_2,
         |    video_one_level_preference_3,
         |    video_two_level_preference_1,
         |    video_two_level_preference_2,
         |    video_two_level_preference_3,
         |    movie_two_level_preference_1,
         |    movie_two_level_preference_2,
         |    movie_two_level_preference_3,
         |    single_two_level_preference_1,
         |    single_two_level_preference_2,
         |    single_two_level_preference_3,
         |    in_package_video_two_level_preference_1,
         |    in_package_video_two_level_preference_2,
         |    in_package_video_two_level_preference_3
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  def saveVideoVector(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_video_vector(
        |            video_id string,
        |            v_0 double,
        |            v_1 double,
        |            v_2 double,
        |            v_3 double,
        |            v_4 double,
        |            v_5 double,
        |            v_6 double,
        |            v_7 double,
        |            v_8 double,
        |            v_9 double,
        |            v_10 double,
        |            v_11 double,
        |            v_12 double,
        |            v_13 double,
        |            v_14 double,
        |            v_15 double,
        |            v_16 double,
        |            v_17 double,
        |            v_18 double,
        |            v_19 double,
        |            v_20 double,
        |            v_21 double,
        |            v_22 double,
        |            v_23 double,
        |            v_24 double,
        |            v_25 double,
        |            v_26 double,
        |            v_27 double,
        |            v_28 double,
        |            v_29 double,
        |            v_30 double,
        |            v_31 double,
        |            v_32 double,
        |            v_33 double,
        |            v_34 double,
        |            v_35 double,
        |            v_36 double,
        |            v_37 double,
        |            v_38 double,
        |            v_39 double,
        |            v_40 double,
        |            v_41 double,
        |            v_42 double,
        |            v_43 double,
        |            v_44 double,
        |            v_45 double,
        |            v_46 double,
        |            v_47 double,
        |            v_48 double,
        |            v_49 double,
        |            v_50 double,
        |            v_51 double,
        |            v_52 double,
        |            v_53 double,
        |            v_54 double,
        |            v_55 double,
        |            v_56 double,
        |            v_57 double,
        |            v_58 double,
        |            v_59 double,
        |            v_60 double,
        |            v_61 double,
        |            v_62 double,
        |            v_63 double)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_video_vector
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license')
         |SELECT
         |    video_id,
         |    v_0,
         |    v_1,
         |    v_2,
         |    v_3,
         |    v_4,
         |    v_5,
         |    v_6,
         |    v_7,
         |    v_8,
         |    v_9,
         |    v_10,
         |    v_11,
         |    v_12,
         |    v_13,
         |    v_14,
         |    v_15,
         |    v_16,
         |    v_17,
         |    v_18,
         |    v_19,
         |    v_20,
         |    v_21,
         |    v_22,
         |    v_23,
         |    v_24,
         |    v_25,
         |    v_26,
         |    v_27,
         |    v_28,
         |    v_29,
         |    v_30,
         |    v_31,
         |    v_32,
         |    v_33,
         |    v_34,
         |    v_35,
         |    v_36,
         |    v_37,
         |    v_38,
         |    v_39,
         |    v_40,
         |    v_41,
         |    v_42,
         |    v_43,
         |    v_44,
         |    v_45,
         |    v_46,
         |    v_47,
         |    v_48,
         |    v_49,
         |    v_50,
         |    v_51,
         |    v_52,
         |    v_53,
         |    v_54,
         |    v_55,
         |    v_56,
         |    v_57,
         |    v_58,
         |    v_59,
         |    v_60,
         |    v_61,
         |    v_62,
         |    v_63
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  /**
   * Save data.
   *
   * @param spark
   * @param df_result
   */
  def saveVideoProfileGenerate(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_video_profile(
        |            video_id string,
        |            video_title string,
        |            video_one_level_classification string,
        |            video_two_level_classification_list array<string>,
        |            video_tag_list array<string>,
        |            director_list array<string>,
        |            actor_list array<string>,
        |            country string,
        |            language string,
        |            release_date string,
        |            storage_time string,
        |            video_time double,
        |            score double,
        |            is_paid double,
        |            package_id string,
        |            is_single double,
        |            is_trailers double,
        |            supplier string,
        |            introduction string,
        |            number_of_plays_in_30_days long,
        |            number_of_views_within_30_days long,
        |            number_of_plays_in_14_days long,
        |            number_of_views_within_14_days long,
        |            number_of_plays_in_7_days long,
        |            number_of_views_within_7_days long,
        |            number_of_plays_in_3_days long,
        |            number_of_views_within_3_days long,
        |            abs_of_number_of_days_between_storage_and_current integer,
        |            number_of_times_purchased_within_30_days long,
        |            number_of_times_purchased_within_14_days long,
        |            number_of_times_purchased_within_7_days long,
        |            number_of_times_purchased_within_3_days long,
        |            number_of_times_purchased_total long)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_video_profile
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |    video_id,
         |    video_title,
         |    video_one_level_classification,
         |    video_two_level_classification_list,
         |    video_tag_list,
         |    director_list,
         |    actor_list,
         |    country,
         |    language,
         |    release_date,
         |    storage_time,
         |    video_time,
         |    score,
         |    is_paid,
         |    package_id,
         |    is_single,
         |    is_trailers,
         |    supplier,
         |    introduction,
         |    number_of_plays_in_30_days,
         |    number_of_views_within_30_days,
         |    number_of_plays_in_14_days,
         |    number_of_views_within_14_days,
         |    number_of_plays_in_7_days,
         |    number_of_views_within_7_days,
         |    number_of_plays_in_3_days,
         |    number_of_views_within_3_days,
         |    abs_of_number_of_days_between_storage_and_current,
         |    number_of_times_purchased_within_30_days,
         |    number_of_times_purchased_within_14_days,
         |    number_of_times_purchased_within_7_days,
         |    number_of_times_purchased_within_3_days,
         |    number_of_times_purchased_total
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  /**
   * Save data.
   *
   * @param spark
   * @param df_result
   */
  def saveUserProfileGeneratePreferencePart(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_profile_preference_part(
        |             user_id string,
        |             total_time_movies_last_30_days double,
        |             total_time_movies_last_14_days double,
        |             total_time_movies_last_7_days double,
        |             total_time_movies_last_3_days double,
        |             total_time_movies_last_1_days double,
        |             total_time_paid_movies_last_30_days double,
        |             total_time_paid_movies_last_14_days double,
        |             total_time_paid_movies_last_7_days double,
        |             total_time_paid_movies_last_3_days double,
        |             total_time_paid_movies_last_1_days double,
        |             active_workdays_last_30_days long,
        |             avg_workdaily_time_videos_last_30_days double,
        |             active_restdays_last_30_days long,
        |             avg_restdaily_time_videos_last_30_days double,
        |             avg_workdaily_time_paid_videos_last_30_days double,
        |             avg_restdaily_time_paid_videos_last_30_days double,
        |             video_one_level_preference map<string, int>,
        |             video_two_level_preference map<string, int>,
        |             tag_preference map<string, int>,
        |             movie_two_level_preference map<string, int>,
        |             movie_tag_preference map<string, int>,
        |             single_two_level_preference map<string, int>,
        |             single_tag_preference map<string, int>,
        |             in_package_video_two_level_preference map<string, int>,
        |             in_package_tag_preference map<string, int>
        |             )
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_profile_preference_part
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |     user_id,
         |     total_time_movies_last_30_days,
         |     total_time_movies_last_14_days,
         |     total_time_movies_last_7_days,
         |     total_time_movies_last_3_days,
         |     total_time_movies_last_1_days,
         |     total_time_paid_movies_last_30_days,
         |     total_time_paid_movies_last_14_days,
         |     total_time_paid_movies_last_7_days,
         |     total_time_paid_movies_last_3_days,
         |     total_time_paid_movies_last_1_days,
         |     active_workdays_last_30_days,
         |     avg_workdaily_time_videos_last_30_days,
         |     active_restdays_last_30_days,
         |     avg_restdaily_time_videos_last_30_days,
         |     avg_workdaily_time_paid_videos_last_30_days,
         |     avg_restdaily_time_paid_videos_last_30_days,
         |     video_one_level_preference,
         |     video_two_level_preference,
         |     tag_preference,
         |     movie_two_level_preference,
         |     movie_tag_preference,
         |     single_two_level_preference,
         |     single_tag_preference,
         |     in_package_video_two_level_preference,
         |     in_package_tag_preference
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  /**
   * Save user profile play data.
   *
   * @param spark
   * @param df_result
   */
  def saveUserProfilePlayData(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_profile_play_part(
        |             user_id string,
        |             active_days_last_30_days long,
        |             total_time_last_30_days double,
        |             days_from_last_active int,
        |             days_since_first_active_in_timewindow int,
        |             active_days_last_14_days long,
        |             total_time_last_14_days double,
        |             active_days_last_7_days long,
        |             total_time_last_7_days double,
        |             active_days_last_3_days long,
        |             total_time_last_3_days double,
        |             total_time_paid_videos_last_30_days double,
        |             total_time_paid_videos_last_14_days double,
        |             total_time_paid_videos_last_7_days double,
        |             total_time_paid_videos_last_3_days double,
        |             total_time_paid_videos_last_1_days double,
        |             total_time_in_package_videos_last_30_days double,
        |             var_time_in_package_videos_last_30_days double,
        |             number_in_package_videos_last_30_days long,
        |             total_time_in_package_videos_last_14_days double,
        |             var_time_in_package_videos_last_14_days double,
        |             number_in_package_videos_last_14_days long,
        |             total_time_in_package_videos_last_7_days double,
        |             var_time_in_package_videos_last_7_days double,
        |             number_in_package_videos_last_7_days long,
        |             total_time_in_package_videos_last_3_days double,
        |             var_time_in_package_videos_last_3_days double,
        |             number_in_package_videos_last_3_days long,
        |             total_time_in_package_videos_last_1_days double,
        |             var_time_in_package_videos_last_1_days double,
        |             number_in_package_videos_last_1_days long,
        |             total_time_children_videos_last_30_days double,
        |             number_children_videos_last_30_days long,
        |             total_time_children_videos_last_14_days double,
        |             number_children_videos_last_14_days long,
        |             total_time_children_videos_last_7_days double,
        |             number_children_videos_last_7_days long,
        |             total_time_children_videos_last_3_days double,
        |             number_children_videos_last_3_days long,
        |             total_time_children_videos_last_1_days double,
        |             number_children_videos_last_1_days long)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_profile_play_part
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |    user_id,
         |    active_days_last_30_days,
         |    total_time_last_30_days,
         |    days_from_last_active,
         |    days_since_first_active_in_timewindow,
         |    active_days_last_14_days,
         |    total_time_last_14_days,
         |    active_days_last_7_days,
         |    total_time_last_7_days,
         |    active_days_last_3_days,
         |    total_time_last_3_days,
         |    total_time_paid_videos_last_30_days,
         |    total_time_paid_videos_last_14_days,
         |    total_time_paid_videos_last_7_days,
         |    total_time_paid_videos_last_3_days,
         |    total_time_paid_videos_last_1_days,
         |    total_time_in_package_videos_last_30_days,
         |    var_time_in_package_videos_last_30_days,
         |    number_in_package_videos_last_30_days,
         |    total_time_in_package_videos_last_14_days,
         |    var_time_in_package_videos_last_14_days,
         |    number_in_package_videos_last_14_days,
         |    total_time_in_package_videos_last_7_days,
         |    var_time_in_package_videos_last_7_days,
         |    number_in_package_videos_last_7_days,
         |    total_time_in_package_videos_last_3_days,
         |    var_time_in_package_videos_last_3_days,
         |    number_in_package_videos_last_3_days,
         |    total_time_in_package_videos_last_1_days,
         |    var_time_in_package_videos_last_1_days,
         |    number_in_package_videos_last_1_days,
         |    total_time_children_videos_last_30_days,
         |    number_children_videos_last_30_days,
         |    total_time_children_videos_last_14_days,
         |    number_children_videos_last_14_days,
         |    total_time_children_videos_last_7_days,
         |    number_children_videos_last_7_days,
         |    total_time_children_videos_last_3_days,
         |    number_children_videos_last_3_days,
         |    total_time_children_videos_last_1_days,
         |    number_children_videos_last_1_days
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  /**
   * Save data to hive.
   *
   * @param spark
   * @param df_result
   */
  def saveUserProfileOrderPart(spark: SparkSession, df_result: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_user_profile_order_part(
        |         user_id string,
        |         number_packages_purchased long,
        |         total_money_packages_purchased double,
        |         max_money_package_purchased double,
        |         min_money_package_purchased double,
        |         avg_money_package_purchased double,
        |         var_money_package_purchased double,
        |         number_singles_purchased long,
        |         total_money_singles_purchased double,
        |         total_money_consumption double,
        |         number_packages_unpurchased long,
        |         money_packages_unpurchased double,
        |         number_singles_unpurchased long,
        |         money_singles_unpurchased double,
        |         days_since_last_purchase_package int,
        |         days_since_last_click_package int,
        |         number_orders_last_30_days long,
        |         number_paid_orders_last_30_days long,
        |         number_paid_package_last_30_days long,
        |         number_paid_single_last_30_days long,
        |         days_remaining_package int)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_result.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_user_profile_order_part
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', category='$category')
         |SELECT
         |    user_id,
         |    number_packages_purchased,
         |    total_money_packages_purchased,
         |    max_money_package_purchased,
         |    min_money_package_purchased,
         |    avg_money_package_purchased,
         |    var_money_package_purchased,
         |    number_singles_purchased,
         |    total_money_singles_purchased,
         |    total_money_consumption,
         |    number_packages_unpurchased,
         |    money_packages_unpurchased,
         |    number_singles_unpurchased,
         |    money_singles_unpurchased,
         |    days_since_last_purchase_package,
         |    days_since_last_click_package,
         |    number_orders_last_30_days,
         |    number_paid_orders_last_30_days,
         |    number_paid_package_last_30_days,
         |    number_paid_single_last_30_days,
         |    days_remaining_package
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  /**
   * Save play data.
   *
   * @param spark
   * @param
   */
  def saveProcessedPlay(spark: SparkSession, df_play: DataFrame, partitiondate: String, license: String) = {

    // 1 - If table not exist, creat.
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_play(
        |         user_id string,
        |         video_id string,
        |         play_end_time string,
        |         broadcast_time double)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    // 2 - Save data.
    println("save data to hive........... \n" * 4)
    df_play.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_play
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  def saveProcessedPlay(spark: SparkSession, df_play: DataFrame, partitiondate: String, license: String, vod_version: String,
                        sector: Int) = {

    // 1 - If table not exist, creat.
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_play_all
        |     (
        |         user_id string,
        |         video_id string,
        |         play_end_time string,
        |         broadcast_time double
        |         )
        |PARTITIONED BY
        |    (partitiondate string, license string, vod_version string, sector int)
      """.stripMargin)

    // 2 - Save data.
    println("save data to hive........... \n" * 4)
    df_play.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_play_all
         |PARTITION
         |    (partitiondate='$partitiondate', license='$license', vod_version='$vod_version', sector=$sector)
         |SELECT
         |    user_id,
         |    video_id,
         |    play_end_time,
         |    broadcast_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)

    println(insert_sql)

    println("over over........... \n" * 4)
  }


  /**
   * Save order data.
   *
   * @param spark
   * @param df_order
   */
  def saveProcessedOrder(spark: SparkSession, df_order: DataFrame, partitiondate: String, license: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_order(
        |         user_id string,
        |         money double,
        |         resource_type double,
        |         resource_id string,
        |         resource_title string,
        |         creation_time string,
        |         discount_description string,
        |         order_status double,
        |         order_start_time string,
        |         order_end_time string)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_order.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_order
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  def saveProcessedOrder(spark: SparkSession, df_order: DataFrame, partitiondate: String, license: String, vod_version: String,
                         sector: Int) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_order_all(
        |         user_id string,
        |         money double,
        |         resource_type double,
        |         resource_id string,
        |         resource_title string,
        |         creation_time string,
        |         discount_description string,
        |         order_status double,
        |         order_start_time string,
        |         order_end_time string)
        |PARTITIONED BY
        |    (partitiondate string, license string, vod_version string, sector int)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_order.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_order_all
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license', vod_version='$vod_version', sector=$sector)
         |SELECT
         |    user_id,
         |    money,
         |    resource_type,
         |    resource_id,
         |    resource_title,
         |    creation_time,
         |    discount_description,
         |    order_status,
         |    order_start_time,
         |    order_end_time
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)

    println(insert_sql)

    println("over over........... \n" * 4)
  }


  def getRawOrders2(spark: SparkSession): DataFrame = {

    val orderRawPath = hdfsPath + "data/train/common/raw/orders/*"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colMoney, StringType),
        StructField(Dic.colResourceId, StringType),
        StructField(Dic.colResourceType, StringType),
        StructField(Dic.colResourceTitle, StringType),
        StructField(Dic.colDiscountDescription, StringType),
        StructField(Dic.colCreationTime, StringType),
        StructField(Dic.colOrderStatus, StringType),
        StructField(Dic.colOrderStartTime, StringType),
        StructField(Dic.colOrderEndTime, StringType),
        StructField(Dic.colPartitionDate, StringType),
        StructField(Dic.colLicense, StringType)
      )
    )

    val df = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .schema(schema)
      .csv(orderRawPath)

    df

  }


  def getRawOrders(spark: SparkSession) = {

    val orderRawPath = hdfsPath + "data/train/common/raw/orders/*"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colMoney, StringType),
        StructField(Dic.colResourceType, StringType),
        StructField(Dic.colResourceId, StringType),
        StructField(Dic.colResourceTitle, StringType),
        StructField(Dic.colCreationTime, StringType),
        StructField(Dic.colDiscountDescription, StringType),
        StructField(Dic.colOrderStatus, StringType),
        StructField(Dic.colOrderStartTime, StringType),
        StructField(Dic.colOrderEndTime, StringType)))

    val df = spark.read
      .option("delimiter", delimiter)
      .option("header", false)
      .schema(schema)
      .csv(orderRawPath)

    df
  }

  def getRawPlays(spark: SparkSession) = {

    val playRawPath = hdfsPath + "data/train/common/raw/plays/*"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colBroadcastTime, FloatType)))

    val df = spark.read
      .option("delimiter", "\\t")
      .option("header", true)
      .schema(schema)
      .csv(playRawPath)

    df
  }

  def getRawPlays2(spark: SparkSession) = {

    val playRawPath = hdfsPath + "data/train/common/raw/plays/*"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colVideoId, StringType),
        StructField(Dic.colPlayEndTime, StringType),
        StructField(Dic.colBroadcastTime, FloatType)))

    val df = spark.read
      .option("delimiter", ",")
      .option("header", true)
      .schema(schema)
      .csv(playRawPath)

    df
  }

  def getRawClickData(spark: SparkSession) = {

    val clicksRawPath = hdfsPath + "data/train/common/raw/clicks/"

    val schema = StructType(
      List(
        StructField(Dic.colUserId, StringType),
        StructField(Dic.colDeviceMsg, StringType),
        StructField(Dic.colFeatureCode, StringType),
        StructField(Dic.colBigVersion, StringType),
        StructField(Dic.colProvince, StringType),
        StructField(Dic.colCity, StringType),
        StructField(Dic.colCityLevel, StringType),
        StructField(Dic.colAreaId, StringType),
        StructField(Dic.colTime, StringType),
        StructField(Dic.colItemType, StringType),
        StructField(Dic.colItemId, StringType)
      )
    )

    val df_raw_click = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(clicksRawPath)


    df_raw_click

  }


  /**
   * Save processed media data to hive
   *
   * @param df_media
   */
  def saveProcessedMedia(spark: SparkSession, df_media: DataFrame, partitiondate: String, license: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_media(
        |            video_id            	string,
        |            video_title         	string,
        |            video_one_level_classification	string,
        |            video_two_level_classification_list	array<string>,
        |            video_tag_list      	array<string>,
        |            director_list       	array<string>,
        |            actor_list          	array<string>,
        |            country             	string,
        |            language            	string,
        |            release_date        	string,
        |            storage_time        	string,
        |            video_time          	double,
        |            score               	double,
        |            is_paid             	double,
        |            package_id          	string,
        |            is_single           	double,
        |            is_trailers         	double,
        |            supplier            	string,
        |            introduction        	string)
        |PARTITIONED BY
        |    (partitiondate string, license string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_media.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_media
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license')
         |SELECT
         |    video_id ,
         |    video_title ,
         |    video_one_level_classification ,
         |    video_two_level_classification_list ,
         |    video_tag_list ,
         |    director_list ,
         |    actor_list ,
         |    country ,
         |    language ,
         |    release_date ,
         |    storage_time ,
         |    video_time ,
         |    score ,
         |    is_paid ,
         |    package_id ,
         |    is_single ,
         |    is_trailers ,
         |    supplier ,
         |    introduction
         |FROM
         |    $tempTable
      """.stripMargin

    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }

  def saveProcessedData(df: DataFrame, path: String) = {

    df.write.mode(SaveMode.Overwrite).format("parquet").save(path)

  }

  def saveLabel(df_label: DataFrame, fileName: String) = {
    val path = hdfsPath + "data/train/common/processed/"
    //    df_label.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(path + fileName)
    df_label.write.mode(SaveMode.Overwrite).format("parquet").save(path + fileName)
  }


  /**
   * Save the tag of video_one_level_classification, video_two_level_classification_list, video_tag_list
   *
   * @param df_label
   * @param category
   */
  def saveLabel(spark: SparkSession, df_label: DataFrame, partitiondate: String, license: String, category: String) = {

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS
        |     vodrs.paypredict_processed_media_label(
        |            content            	string)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_label.createOrReplaceTempView(tempTable)

    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.paypredict_processed_media_label
         |PARTITION
         |    (partitiondate = '$partitiondate', license = '$license', category = '$category')
         |SELECT
         |    content
         |FROM
         |    $tempTable
      """.stripMargin
    spark.sql(insert_sql)
    println("over over........... \n" * 4)
  }


  /**
   * @description: saveProcessedMedia to HDFS
   * @param: df_processed_media
   * @return: void
   * @author: wx
   * @Date: 2021/1/4
   */
  def saveProcessedMedia(df_processed_media: DataFrame) = {


    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp"

    saveProcessedData(df_processed_media, mediasProcessedPath)
  }

  /**
   * @description: saveProcessedOrder to HDFS
   * @param: df_order_processed
   * @return: void
   * @author: wx
   * @Date: 2021/1/4
   */
  def saveProcessedOrder(df_order_processed: DataFrame) = {


    val orderProcessedPath = hdfsPath + "data/train/common/processed/orders"
    saveProcessedData(df_order_processed, orderProcessedPath)
  }

  /**
   * @description: saveProcessedPlay to HDFS
   * @param: spark
   * @param: license
   * @param: category
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: wx
   * @Date: 2021/1/4
   */
  def saveProcessedPlay(df_play_processed: DataFrame) = {

    val playProcessedPath = hdfsPath + "data/train/common/processed/plays"
    df_play_processed.write.mode(SaveMode.Overwrite).format("parquet").save(playProcessedPath)
  }


  /**
   * @description: getProcessedMedias from HDFS
   * @param: sparkSession
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: wx
   * @Date: 2021/1/4
   */
  def getProcessedMedias(sparkSession: SparkSession) = {

    val mediasProcessedPath = hdfsPath + "data/train/common/processed/mediastemp/*"
    sparkSession.read.format("parquet").load(mediasProcessedPath)

  }

  /**
   * @description: getProcessedOrder from HDFS
   * @param: spark
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: wx
   * @Date: 2021/1/4
   */
  def getProcessedOrder(spark: SparkSession) = {

    val ordersProcessedPath = hdfsPath + "data/train/common/processed/orders"
    spark.read.format("parquet").load(ordersProcessedPath)

  }

  def getProcessedPlay(sparkSession: SparkSession) = {

    val playsProcessedPath = hdfsPath + "data/train/common/processed/plays"
    sparkSession.read.format("parquet").load(playsProcessedPath)

  }

  /**
   * @description: All Users' id get from Hisense
   * @param: spark
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: wx
   * @Date: 2021/1/4
   */
  def getAllUsers(spark: SparkSession) = {

    val allUsersPath = hdfsPath + "data/train/userpay/allUsers/user_id.txt"
    spark.read.format("csv").load(allUsersPath).toDF(Dic.colUserId)

  }

  def getAllUsersPlayAndOrder(spark: SparkSession) = {
    val df_play = getProcessedPlay(spark)
    val df_play_id = df_play.select(Dic.colUserId).dropDuplicates()
    val df_order = getProcessedOrder(spark)
    val df_order_id = df_order.select(Dic.colUserId).dropDuplicates()
    val df_all_id = df_order_id.union(df_play_id).dropDuplicates()

    df_all_id

  }

  /**
   * @description: save all Train Users to HDFS
   * @param: trainTime
   * @param: df_all_train_users
   * @return: void
   * @author: wx
   * @Date: 2021/1/4
   */
  def saveTrainUsers(trainTime: String, df_all_train_users: DataFrame) = {

    val trainSetUsersPath = hdfsPath + "data/train/xxkang/userpay/trainUsers" + trainTime.split(" ")(0)
    saveProcessedData(df_all_train_users, trainSetUsersPath)
  }


  def savePredictUsers(predictTime: String, df_all_predict_users: DataFrame) = {
    val predictSetUsersPath = hdfsPath + "data/predict/xxkang/userpay/predictUsers" + predictTime.split(" ")(0)
    saveProcessedData(df_all_predict_users, predictSetUsersPath)
  }

  /**
   * @description: Train User
   * @param: sparkSession
   * @param: now
   * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @author: wx
   * @Date: 2021/1/4
   */
  def getTrainUser(sparkSession: SparkSession, now: String) = {
    val trainUsersPath = hdfsPath + "data/train/xxkang/userpay/trainUsers" + now.split(" ")(0)
    getData(sparkSession, trainUsersPath)
  }

  def getPredictUser(sparkSession: SparkSession, now: String) = {
    val predictUsersPath = hdfsPath + "data/predict/xxkang/userpay/predictUsers" + now.split(" ")(0)
    getData(sparkSession, predictUsersPath)
  }

  /**
   * User Profile Data Save Functions and Get Functions
   */
  def saveUserProfileOrderPart(now: String, df_user_profile_order: DataFrame, state: String) = {

    val userProfileOrderPartSavePath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofileorderpart" + now.split(" ")(0)
    saveProcessedData(df_user_profile_order, userProfileOrderPartSavePath)
  }

  def getUserProfileOrderPart(sparkSession: SparkSession, now: String, state: String) = {
    val userProfileOrderPartPath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofileorderpart" + now.split(" ")(0)
    getData(sparkSession, userProfileOrderPartPath)
  }

  def saveUserProfilePlayPart(now: String, df_user_profile_play: DataFrame, state: String) = {

    val userProfilePlayPartSavePath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofileplaypart" + now.split(" ")(0)

    saveProcessedData(df_user_profile_play, userProfilePlayPartSavePath)
  }


  def getUserProfilePlayPart(sparkSession: SparkSession, now: String, state: String) = {

    val userProfilePlayPartPath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofileplaypart" + now.split(" ")(0)
    getData(sparkSession, userProfilePlayPartPath)
  }


  def saveUserProfilePreferencePart(now: String, df_user_profile_pf: DataFrame, state: String) = {

    val userProfilePreferencePartSavePath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofilepreferencepart" + now.split(" ")(0)

    saveProcessedData(df_user_profile_pf, userProfilePreferencePartSavePath)
  }


  def getUserProfilePreferencePart(sparkSession: SparkSession, now: String, state: String) = {
    val userProfilePreferencePartSavePath = hdfsPath + "data/" + state + "/xxkang/userpay/userprofilepreferencepart" + now.split(" ")(0)
    getData(sparkSession, userProfilePreferencePartSavePath)
  }

  /**
   * Medias Label Get Functions
   */

  def getVideoFirstCategory() = {
    val videoFirstCategoryTempPath = hdfsPath + "data/train/common/processed/videofirstcategorytemp"
    //    spark.read.option("header", true).format("csv").load(videoFirstCategoryTempPath)
    spark.read.format("parquet").load(videoFirstCategoryTempPath)

  }

  def getVideoSecondCategory() = {

    val videoSecondCategoryTempPath = hdfsPath + "data/train/common/processed/videosecondcategorytemp"
    //    spark.read.option("header", true).format("csv").load(videoSecondCategoryTempPath)
    spark.read.format("parquet").load(videoSecondCategoryTempPath)


  }


  def getVideoLabel() = {

    val labelTempPath = hdfsPath + "data/train/common/processed/labeltemp"
    //    spark.read.option("header", true).format("csv").load(labelTempPath)
    spark.read.format("parquet").load(labelTempPath)

  }

  /**
   * Train Set
   */
  def saveDataSet(now: String, df_data: DataFrame, state: String) = {
    val Path = hdfsPath + "data/" + state + "/xxkang/userpay/" + state + "UserProfile" + now.split(" ")(0)
    saveProcessedData(df_data, Path)
  }

  def getDataSet(now: String, state: String) = {
    val Path = hdfsPath + "data/" + state + "/xxkang/userpay/" + state + "UserProfile" + now.split(" ")(0)
    getData(spark, Path)

  }


  def getRawMediaData2(spark: SparkSession) = {

    //
    val mediasRawPath = hdfsPath + "data/train/common/raw/medias/"

    val schema = StructType(
      List(
        StructField("media_id", StringType),
        StructField("c_1", StringType),
        StructField("type_name", StringType),
        StructField("category_name_array", StringType),
        StructField("tag_name_array", StringType),
        StructField("director_name_array", StringType),
        StructField("actor_name_array", StringType),
        StructField(Dic.colCountry, StringType),
        StructField(Dic.colLanguage, StringType),
        StructField("pubdate", StringType),
        StructField("created_time", StringType),
        //视频时长
        StructField("time_length", StringType),
        StructField("rate", StringType),
        StructField("fee", StringType),
        StructField("vip_id", StringType),
        StructField("is_single", StringType),
        //是否片花
        StructField("is_clip", StringType),
        StructField("vender_name", StringType),
        StructField("_c18", StringType)
      )
    )

    // Konverse - 注意 df 的命名 - df_相关属性 - 不要 dfRawMedia
    val df_raw_media = spark.read
      .option("delimiter", "\\t")
      .option("header", true)
      .schema(schema)
      .csv(mediasRawPath)
      .withColumnRenamed("media_id", Dic.colVideoId)
      .withColumnRenamed("c_1", Dic.colVideoTitle)
      .withColumnRenamed("type_name", Dic.colVideoOneLevelClassification)
      .withColumnRenamed("category_name_array", Dic.colVideoTwoLevelClassificationList)
      .withColumnRenamed("tag_name_array", Dic.colVideoTagList)
      .withColumnRenamed("director_name_array", Dic.colDirectorList)
      .withColumnRenamed("actor_name_array", Dic.colActorList)
      .withColumnRenamed("pubdate", Dic.colReleaseDate)
      .withColumnRenamed("created_time", Dic.colStorageTime)
      .withColumnRenamed("time_length", Dic.colVideoTime)
      .withColumnRenamed("rate", Dic.colScore)
      .withColumnRenamed("fee", Dic.colIsPaid)
      .withColumnRenamed("vip_id", Dic.colPackageId)
      .withColumnRenamed("is_single", Dic.colIsSingle)
      .withColumnRenamed("is_clip", Dic.colIsTrailers)
      .withColumnRenamed("vender_name", Dic.colSupplier)
      .withColumnRenamed("_c18", Dic.colIntroduction)

    df_raw_media

  }

  //  Save user info get from click data
  def saveProcessedUserMeta(df_data: DataFrame) = {

    val path = hdfsPath + "data/common/xxkang/clickMetaData"
    saveProcessedData(df_data, path)

  }


  //  Save user info get from click data
  def getProcessedUserMeta() = {

    val path = hdfsPath + "data/common/xxkang/clickMetaData"
    getData(spark, path)
  }

  def saveCSVFile(df: DataFrame, path: String) = {

    df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(path)
  }


  def saveUserNode(df: DataFrame, state: String, fileName: String) = {
    val path = hdfsPath + "data/" + state + "/xxkang/userVector/" + fileName
    saveCSVFile(df, path)
  }

}
