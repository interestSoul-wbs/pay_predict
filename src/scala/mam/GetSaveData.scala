package mam

import org.apache.ivy.core.module.descriptor.License
import org.apache.spark.sql.{DataFrame, SparkSession}

object GetSaveData {

  var tempTable = "temp_table"

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
    * Get processed media data.
    *
    * @param spark
    * @return
    */
  def getMediasProcessed(spark: SparkSession, partitiondate: String, license: String) = {

    val medias_sql =
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
         |    vodrs.t_media_sum_processed_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_medias = spark.sql(medias_sql)

    df_medias
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
        |     vodrs.t_media_label_paypredict(
        |            content            	string)
        |PARTITIONED BY
        |    (partitiondate string, license string, category string)
      """.stripMargin)

    println("save data to hive........... \n" * 4)
    df_label.createOrReplaceTempView(tempTable)
    val insert_sql =
      s"""
         |INSERT OVERWRITE TABLE
         |    vodrs.t_media_label_paypredict
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
         |    vodrs.t_sdu_user_order_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_order = spark.sql(user_order_sql)

    df_order
  }

  /**
    * 这是已经抽样的用户在一定时间段内的 play 数据，是抽取给山大的数据。
    * 这里的subscriberid 是 rank，要拿到实际的 subscriberid 需要 通过 vodrs.t_vod_user_sample_sdu_v1 join
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
         |    partitiondate>='$start_date' and partitiondate<='$end_date' license='$license'
      """.stripMargin

    val df_play = spark.sql(user_play_sql)

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
         |    vodrs.t_sdu_user_play_history_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
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
  def getUserProfilePlayPart(spark: SparkSession, partitiondate: String, license: String, data_type: String) = {

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
         |    vodrs.t_sdu_user_profile_play_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license' and data_type='$data_type'
      """.stripMargin

    val df_user_profile_play = spark.sql(data_sql)

    df_user_profile_play
  }


  /**
    * 这是已经抽样的用户在一定时间段内的订单，是抽取给山大的数据。
    * 这里的subscriberid 是 rank，要拿到实际的 subscriberid 需要 通过 vodrs.t_vod_user_sample_sdu_v1 join
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
         |    partitiondate<='$start_date' and partitiondate>='$end_date' and licence='$license'
      """.stripMargin

    val df_order_ori = spark.sql(sample_user_order_ori_sql)

    df_order_ori
  }

  /**
    * Get smaple users' order data within a period.
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
         |    partitiondate<='$start_date' and partitiondate>='$today' and licence='$license'
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


  def getSampleUser(spark: SparkSession, today: String, license: String) = {

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
         |    vodrs.t_media_sum_processed_paypredict
         |WHERE
         |    partitiondate='$partitiondate' and license='$license'
      """.stripMargin

    val df_medias = spark.sql(user_order_sql)

    df_medias
  }

}
