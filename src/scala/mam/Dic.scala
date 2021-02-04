package mam

/**
  * 命名规则：
  * 变量名称都以 col 开头，然后驼峰命名；
  * 左边是直观看到的，规定好的，因为为了美观和方便修改，因此定义；
  * 右边是 代码实际运行时的变量名称；
  */
object Dic {
  /**
    * play数据集中的属性
    */
  //原始属性
  val colUserId = "user_id"
  val colPlayEndTime = "play_end_time"
  val colBroadcastTime = "broadcast_time"
  //处理过程中添加
  val colPlayStartTime = "play_start_time"
  val colTimeSum = "time_sum"
  val colStartTimeLeadSameVideo = "start_time_lead_Same_video"
  val colStartTimeLeadPlay = "start_time_lead_play"
  val colTimeGapLeadSameVideo = "time_gap_lead_same_video"
  val colTimeGap30minSign = "timeGap_30min_sign"
  val colTimeGap30minSignLag = "timeGap_30min_sign_lag"
  val colTimeGap3hSign = "timeGap_3h_sign"
  val colTimeGap3hSignLag = "timeGap_3h_sign_lag"
  val colConvertEndTime = "convert_end_time"
  val colSessionSign = "session_sign"
  val colConvertTime = "convert_time"
  val colCreationGap = "creation_gap"
  val colPlayDate = "play_date"

  /**
    * medias数据集中的属性
    *
    */
  val colVideoId = "video_id"
  val colVideoTitle = "video_title"
  val colVideoOneLevelClassification = "video_one_level_classification"
  val colVideoTwoLevelClassificationList = "video_two_level_classification_list"
  val colVideoTagList = "video_tag_list"
  val colDirectorList = "director_list"
  val colActorList = "actor_list"
  val colCountry = "country"
  val colLanguage = "language"
  val colReleaseDate = "release_date"
  val colStorageTime = "storage_time"
  val colVideoTime = "video_time"
  val colScore = "score"
  val colIsPaid = "is_paid"
  val colPackageId = "package_id"
  val colIsSingle = "is_single"
  val colIsTrailers = "is_trailers"
  val colSupplier = "supplier"
  val colIntroduction = "introduction"

  /**
    * order中的属性
    */
  val colResourceId = "resource_id"
  val colResourceTitle = "resource_title"
  val colDiscountDescription = "discount_description"
  val colCreationTime = "creation_time"
  val colResourceType = "resource_type"
  val colOrderStatus = "order_status"
  val colMoney = "money"
  val colOrderStartTime = "order_start_time"
  val colOrderEndTime = "order_end_time"
  val colTimeValidity = "time_validity" //有效时长
  val colKeepSign = "keep_sign" //筛选数据使用
  val colIsMoneyError = "is_money_error" //金额信息异常标注
  val colCreationTimeGap = "creation_time_gap"
  val colOrderHistory = "order_history"

  /**
    * 用户画像中的属性
    */
  //最近30天的活跃天数
  val colActiveDaysLast30Days = "active_days_last_30_days"
  //最近30天的总的观看时间
  val colTotalTimeLast30Days = "total_time_last_30_days"
  //从上一次活跃日期到现在的天数
  val colDaysFromLastActive = "days_from_last_active"
  //时间窗口内第一次活跃的日期到现在的天数
  val colDaysSinceFirstActiveInTimewindow = "days_since_first_active_in_timewindow"
  //最近14天的活跃天数
  val colActiveDaysLast14Days = "active_days_last_14_days"
  //最近14天总的观看时间
  val colTotalTimeLast14Days = "total_time_last_14_days"
  //最近7天的活跃天数
  val colActiveDaysLast7Days = "active_days_last_7_days"
  //最近7天总的观看时间
  val colTotalTimeLast7Days = "total_time_last_7_days"
  //最近3天的活跃天数
  val colActiveDaysLast3Days = "active_days_last_3_days"
  //最近3天总的观看时间
  val colTotalTimeLast3Days = "total_time_last_3_days"


  //用户最近30天的观看付费视频总时长
  val colTotalTimePaidVideosLast30Days = "total_time_paid_videos_last_30_days"
  //用户最近14天的观看付费视频总时长
  val colTotalTimePaidVideosLast14Days = "total_time_paid_videos_last_14_days"
  //用户最近7天的观看付费视频总时长
  val colTotalTimePaidVideosLast7Days = "total_time_paid_videos_last_7_days"
  //用户最近3天的观看付费视频总时长
  val colTotalTimePaidVideosLast3Days = "total_time_paid_videos_last_3_days"
  //用户最近1天的观看付费视频总时长
  val colTotalTimePaidVideosLast1Days = "total_time_paid_videos_last_1_days"

  //用户一段时间内的观看套餐内视频总时长、方差、数量(30,14,7,3,1)
  val colTotalTimeInPackageVideosLast30Days = "total_time_in_package_videos_last_30_days"
  val colVarTimeInPackageVideosLast30Days = "var_time_in_package_videos_last_30_days"
  val colNumberInPackagesVideosLast30Days = "number_in_package_videos_last_30_days"

  val colTotalTimeInPackageVideosLast14Days = "total_time_in_package_videos_last_14_days"
  val colVarTimeInPackageVideosLast14Days = "var_time_in_package_videos_last_14_days"
  val colNumberInPackagesVideosLast14Days = "number_in_package_videos_last_14_days"

  val colTotalTimeInPackageVideosLast7Days = "total_time_in_package_videos_last_7_days"
  val colVarTimeInPackageVideosLast7Days = "var_time_in_package_videos_last_7_days"
  val colNumberInPackagesVideosLast7Days = "number_in_package_videos_last_7_days"

  val colTotalTimeInPackageVideosLast3Days = "total_time_in_package_videos_last_3_days"
  val colVarTimeInPackageVideosLast3Days = "var_time_in_package_videos_last_3_days"
  val colNumberInPackagesVideosLast3Days = "number_in_package_videos_last_3_days"

  val colTotalTimeInPackageVideosLast1Days = "total_time_in_package_videos_last_1_days"
  val colVarTimeInPackageVideosLast1Days = "var_time_in_package_videos_last_1_days"
  val colNumberInPackagesVideosLast1Days = "number_in_package_videos_last_1_days"


  //观看幼儿类视频的时长和个数(30,14,7,3,1)
  val colTotalTimeChildrenVideosLast30Days = "total_time_children_videos_last_30_days"
  val colNumberChildrenVideosLast30Days = "number_children_videos_last_30_days"
  val colTotalTimeChildrenVideosLast14Days = "total_time_children_videos_last_14_days"
  val colNumberChildrenVideosLast14Days = "number_children_videos_last_14_days"
  val colTotalTimeChildrenVideosLast7Days = "total_time_children_videos_last_7_days"
  val colNumberChildrenVideosLast7Days = "number_children_videos_last_7_days"
  val colTotalTimeChildrenVideosLast3Days = "total_time_children_videos_last_3_days"
  val colNumberChildrenVideosLast3Days = "number_children_videos_last_3_days"
  val colTotalTimeChildrenVideosLast1Days = "total_time_children_videos_last_1_days"
  val colNumberChildrenVideosLast1Days = "number_children_videos_last_1_days"


  //电影观看总时长
  val colTotalTimeMoviesLast30Days = "total_time_movies_last_30_days"
  val colTotalTimeMoviesLast14Days = "total_time_movies_last_14_days"
  val colTotalTimeMoviesLast7Days = "total_time_movies_last_7_days"
  val colTotalTimeMoviesLast3Days = "total_time_movies_last_3_days"
  val colTotalTimeMoviesLast1Days = "total_time_movies_last_1_days"

  //付费电影观看总时长
  val colTotalTimePaidMoviesLast30Days = "total_time_paid_movies_last_30_days"
  val colTotalTimePaidMoviesLast14Days = "total_time_paid_movies_last_14_days"
  val colTotalTimePaidMoviesLast7Days = "total_time_paid_movies_last_7_days"
  val colTotalTimePaidMoviesLast3Days = "total_time_paid_movies_last_3_days"
  val colTotalTimePaidMoviesLast1Days = "total_time_paid_movies_last_1_days"

  //近一个月工作日和休息日的观看情况
  val colActiveWorkdaysLast30Days = "active_workdays_last_30_days"
  val colActiveRestdaysLast30Days = "active_restdays_last_30_days"
  val colAvgWorkdailyTimeVideosLast30Days = "avg_workdaily_time_videos_last_30_days"
  val colAvgRestdailyTimeVideosLast30Days = "avg_restdaily_time_videos_last_30_days"
  val colAvgWorkdailyTimePaidVideosLast30Days = "avg_workdaily_time_paid_videos_last_30_days"
  val colAvgRestdailyTimePaidVideosLast30Days = "avg_restdaily_time_paid_videos_last_30_days"

  //各种偏好
  val colVideoOneLevelPreference = "video_one_level_preference"
  val colVideoTwoLevelPreference = "video_two_level_preference"
  val colTagPreference = "tag_preference"
  val colMovieTwoLevelPreference = "movie_two_level_preference"
  val colMovieTagPreference = "movie_tag_preference"
  val colSingleTwoLevelPreference = "single_two_level_preference"
  val colSingleTagPreference = "single_tag_preference"


  val colInPackageVideoTwoLevelPreference = "in_package_video_two_level_preference"
  val colInPackageTagPreference = "in_package_tag_preference"


  //
  val colNumberPackagesPurchased = "number_packages_purchased"
  val colTotalMoneyPackagesPurchased = "total_money_packages_purchased"
  val colMaxMoneyPackagePurchased = "max_money_package_purchased"
  val colMinMoneyPackagePurchased = "min_money_package_purchased"
  val colAvgMoneyPackagePurchased = "avg_money_package_purchased"
  val colVarMoneyPackagePurchased = "var_money_package_purchased"
  val colNumberSinglesPurchased = "number_singles_purchased"
  val colTotalMoneySinglesPurchased = "total_money_singles_purchased"
  val colTotalMoneyConsumption = "total_money_consumption"

  val colNumberPackagesUnpurchased = "number_packages_unpurchased"
  val colMoneyPackagesUnpurchased = "money_packages_unpurchased"
  val colNumberSinglesUnpurchased = "number_singles_unpurchased"
  val colMoneySinglesUnpurchased = "money_singles_unpurchased"

  val colDaysSinceLastPurchasePackage = "days_since_last_purchase_package"
  val colDaysSinceLastClickPackage = "days_since_last_click_package"

  val colNumbersOrdersLast30Days = "number_orders_last_30_days"
  val colNumberPaidOrdersLast30Days = "number_paid_orders_last_30_days"
  val colNumberPaidPackageLast30Days = "number_paid_package_last_30_days"
  val colNumberPaidSingleLast30Days = "number_paid_single_last_30_days"

  val colDaysRemainingPackage = "days_remaining_package"


  /**
    * 视频画像中的属性
    */

  val colNumberOfPlaysIn30Days = "number_of_plays_in_30_days"
  val colNumberOfViewsWithin30Days = "number_of_views_within_30_days"
  val colNumberOfPlaysIn14Days = "number_of_plays_in_14_days"
  val colNumberOfViewsWithin14Days = "number_of_views_within_14_days"
  val colNumberOfPlaysIn7Days = "number_of_plays_in_7_days"
  val colNumberOfViewsWithin7Days = "number_of_views_within_7_days"
  val colNumberOfPlaysIn3Days = "number_of_plays_in_3_days"
  val colNumberOfViewsWithin3Days = "number_of_views_within_3_days"
  val colAbsOfNumberOfDaysBetweenStorageAndCurrent = "abs_of_number_of_days_between_storage_and_current"


  val colNumberOfTimesPurchasedWithin30Days = "number_of_times_purchased_within_30_days"
  //val colNumberOfOrdersWithin30Days="number_of_orders_within_30_days"
  val colNumberOfTimesPurchasedWithin14Days = "number_of_times_purchased_within_14_days"
  // val colNumberOfOrdersWithin14Days="number_of_orders_within_14_days"
  val colNumberOfTimesPurchasedWithin7Days = "number_of_times_purchased_within_7_days"
  //val colNumberOfOrdersWithin7Days="number_of_orders_within_7_days"
  val colNumberOfTimesPurchasedWithin3Days = "number_of_times_purchased_within_3_days"
  //val colNumberOfOrdersWithin3Days="number_of_orders_within_3_days"
  val colNumberOfTimesPurchasedTotal = "number_of_times_purchased_total"


  /**
    * click中的用户属性
    */
  //"subscriberid","devicemsg","featurecode","bigversion","province","city","citylevel","areaid",
  // "time","itemtype","itemid","partitiondate"
  val colDeviceMsg = "devicemsg"
  val colFeatureCode = "featurecode"
  val colBigVersion = "bigversion"
  val colProvince = "province"
  val colCity = "city"
  val colCityLevel = "citylevel"
  val colAreaId = "areaid"
  val colTime = "time"
  val colItemType = "itemtype"
  val colItemId = "itemid"
  val colPartitionDate = "partitiondate"

  val colRank = "rank"
  val colContent = "content"
  val colLabel = "label"
  val colFirst = "first"
  val colSecond = "second"
  val colOneLevel = "one_level"
  val colTwoLevel = "two_level"
  val colVideoTag = "video_tag"
  val colSubId = "sub_id"
  val colSubscriberid = "subscriberid"
  val colCustomerid: String = "customerid"
  val colDeviceid: String = "deviceid"
  val colShuntSubid = "shunt_subid"
  val colSector = "sector"
  val colFee = "fee"
  val colResourcetype = "resourcetype"
  val colResourceid = "resourceid"
  val colResourcename = "resourcename"
  val colCreatedtime = "createdtime"
  val colDiscountdesc = "discountdesc"
  val colStatus = "status"
  val colStarttime = "starttime"
  val colEndtime = "endtime"
  val colDiscountid = "discountid"
  val colVideoList = "video_list"
  val colVector: String = "vector"
  val colVectors: String = "vectors"
  val colWord: String = "word"
  val colPlayMonth: String = "play_month"
  val colResult: String = "result"
  val colItemid: String = "itemid"
  val colDuration: String = "duration"
  val colIsForMattedTime: String = "is_formatted_time"
  val colIsLongtypeTimePattern1: String = "is_longtype_time_pattern_1"
  val colIsLongtypeTimePattern2: String = "is_longtype_time_pattern_2"
  val colIsOnlyNumberUserId: String = "is_only_number_user_id"
  val colIsOnlyNumberVideoId: String = "is_only_number_video_id"
  val colIsOnlyNumberBroadcastTime: String = "is_only_number_broadcast_time"
  val colIsOnlyNumberVideoTime: String = "is_only_number_video_time"
  val colIsOnlyNumberResourceId: String = "is_only_number_resource_id"
  val colIsForMattedTimeReleaseDate: String = "is_formatted_time_release_date"
  val colIsForMattedTimeStorageTime : String = "is_formatted_time_storage_time"
  val colIsLongtypeTimeStorageTime: String = "is_longtype_time_storage_time"
  val colIsLongtypeTimeCreationTime: String = "is_longtype_time_creation_time"
  val colIsFormattedTimePlayEndTime: String = "is_formatted_time_play_end_time"
  val colIsLongtypeTimeOrderStartTime: String = "is_longtype_time_order_start_time"
  val colIsLongtypeTimeOrderEndTime: String = "is_longtype_time_order_end_time"
  val colCount: String = "count"
  val colLicense: String = "license"
  val colCategory: String = "category"
  val colFeatures: String = "features"
  val colScaledFeatures: String = "scaled_features"
  val colKeepOriginalFeatures: String = "keep_original_features"
  val colTmp: String = "tmp"
  val colPartitiondate: String = "partitiondate"
  val colVodVersion: String = "vod_version"
  val colPlayEndTimeTmp = "play_end_time_tmp"
}
