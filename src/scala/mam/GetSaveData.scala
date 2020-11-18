package mam

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

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

  def getRawMediaData(spark: SparkSession) = {

    val hdfsPath = "hdfs:///pay_predict/"
    //val hdfsPath=""
    val mediasRawPath = hdfsPath + "data/train/common/raw/medias/medias.txt"

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
    val dfRawMedias = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .schema(schema)
      .csv(mediasRawPath)

    dfRawMedias
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


  def getuserProfilePreferencePart(spark: SparkSession, partitiondate: String, license: String, category: String) = {

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
}
