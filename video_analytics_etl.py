'''
This script is
- Builds a spark session
- Extracting raw data from the video analytics parquet file
- Transforming raw data to meaningful downstream query aggregation format
- Loading/writing analytical aggregated data
TODO:
1: Implement Unit Test cases
2: Code restructuring extending base class implementing ETL
3: Optimize - TOO many object creation
4: etc...
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging

# Building the Sparksession
spark = SparkSession.builder.appName('video analytics ETL').getOrCreate()
spark.sparkContext.setLogLevel("INFO")


def createDimDate(df):
    '''
    Creates date dimensional table from DateTime field in upstream dataframe
    :param df:
    :return: Date Dimensional Dataframe
    '''
    df = df.withColumn("rawKey", F.col('rawKey'))
    df = df.withColumn("year", F.year(F.col('DateTime')))
    df = df.withColumn("month", F.month(F.col('DateTime')))
    df = df.withColumn("dayofmonth", F.dayofmonth(F.col('DateTime')))
    df = df.withColumn("dayofweek", F.dayofweek(F.col('DateTime')))
    df = df.withColumn("dayofyear", F.dayofyear(F.col('DateTime')))
    df = df.withColumn("hour", F.hour(F.col('DateTime')))
    df = df.withColumn("minute", F.minute(F.col('DateTime')))
    df = df.withColumn("dateMinute", F.date_format(F.col("DateTime"), "yyyyMMddHHmm"))
    df = df.withColumn("quarter", F.quarter(F.col('DateTime')))
    df = df.withColumn("date", F.to_date(F.col('DateTime')))
    df.createOrReplaceTempView('tempDimDateTable')
    dimDateDF = spark.sql(" SELECT * FROM \
                    (select rawKey,dateMinute,dateTime, date,year, month,dayofmonth,dayofweek,dayofyear,hour, minute,quarter \
                    from tempDimDateTable \
                    group by rawKey,dateMinute,dateTime, date,year, month,dayofmonth,dayofweek,dayofyear,hour, minute,quarter \
                    order by dateMinute ASC) \
                    ")

    # Generating dateKey field
    dimDateDF = dimDateDF.withColumn('dateKey', F.monotonically_increasing_id() + 1)
    # Creating dataframe including date field which will help to generate Fact table
    factHelperDateDF = dimDateDF.select(F.col('rawKey'), F.col('dateKey'), F.col('dateMinute'))
    # Dropping unnecessary rawKey field
    dimDateDF = dimDateDF.drop(F.col('rawKey'))
    return dimDateDF, factHelperDateDF


def createDimPlatform(df):
    '''
    Creates platform dimensional table from VideoTitle field in upstream dataframe
    :param df:
    :return: Platform Dimensional Dataframe
    '''
    # Splitting videoTitle column with pipe(|)
    videoTitle = F.split(df['VideoTitle'], "\\|")
    df = df.withColumn("split_count", F.size(videoTitle))
    # discarding where split_count is less than 1
    df = df.where("split_count>1")
    df = df.drop('split_count')
    withFirstItemDF = df.withColumn("firstItem", videoTitle.getItem(0))

    withFirstItemDF.createOrReplaceTempView('withFirstItemTable')
    dimPlatformDF = spark.sql(" select \
              firstItem as platformName, \
             CASE \
              WHEN lower(firstItem) like ('%pad%') Then firstItem \
              WHEN lower(firstItem) like ('%phone%') Then firstItem \
              WHEN lower(firstItem) like ('%app%') Then firstItem \
              WHEN firstItem is not null Then 'Desktop' \
              Else firstItem \
              END AS platform \
              from withFirstItemTable \
              group by  firstItem, platform")
    # Generating platformKey field
    dimPlatformDF = dimPlatformDF.withColumn('platformKey', F.monotonically_increasing_id() + 1)
    dimPlatformDF.createOrReplaceTempView('dimPlatformTable')
    # Creating fact helper DF including Platform field which will help to create fact field in downstream
    factHelperPlatformDF = spark.sql("select a.rawKey, b.platformKey from \
                               withFirstItemTable a \
                              JOIN \
                              dimPlatformTable b ON \
                              a.firstItem=b.platformName"
                                     ).distinct()
    return dimPlatformDF, factHelperPlatformDF


def createDimSite(df):
    '''
    Creates Site dimensional table from VideoTitle field in upstream dataframe
    :param df:
    :return: Site Dimensional Dataframe
    '''
    # Splitting videoTitle column with pipe(|)
    videoTitle = F.split(df['VideoTitle'], "\\|")
    df = df.withColumn("split_count", F.size(videoTitle))
    # discarding where split_count is less than 1
    df = df.where("split_count>1")
    df = df.drop('split_count')
    withFirstItemDF = df.withColumn("firstItem", videoTitle.getItem(0))
    withFirstItemDF.createOrReplaceTempView('withFirstItemTable')
    dimSiteDF = spark.sql(" select * FROM \
                 (select \
                  CASE  \
                  WHEN lower(firstItem) not like ('%pad%') AND \
                  lower(firstItem) not like ('%phone%') AND \
                  lower(firstItem) not like ('%app%') \
                  Then firstItem  \
                  END AS siteName \
                  from withFirstItemTable \
                  group by firstItem) \
                  WHERE siteName is not null")
    # Generating SiteKey field
    dimSiteDF = dimSiteDF.withColumn('siteKey', F.monotonically_increasing_id() + 1)
    dimSiteDF = dimSiteDF.select(F.col('siteKey'), F.col('siteName'))

    dimSiteDF.createOrReplaceTempView('dimSiteTable')
    # Creating fact helper DF including Site field which will help to create fact field in downstream
    factHelperPlatformDF = spark.sql("Select a.rawKey, b.siteName,b.siteKey from \
                                     withFirstItemTable a JOIN \
                                     dimSiteTable b ON \
                                     a.firstItem=b.siteName").distinct()
    return dimSiteDF, factHelperPlatformDF


def createDimVideo(df):
    '''
    Creates video dimensional table from VideoTitle field in upstream dataframe
    :param df:
    :return: Video Dimensional Dataframe
    '''
    # Splitting videoTitle column with pipe(|)
    videoTitle = F.split(df['VideoTitle'], "\\|")
    df = df.withColumn("split_count", F.size(videoTitle))
    # discarding where split_count is less than 1
    df = df.where("split_count>1")
    df = df.drop('split_count')
    splitted = df.select(F.split(df['VideoTitle'], "\\|").alias('arr'), F.col('rawKey'))
    video = splitted.select((splitted.arr[F.size(splitted.arr) - 1]).alias('videoName'), F.col('rawKey'))
    video.createOrReplaceTempView('videoTable')
    # Generating videoKey field
    dimVideoDF = video.select(F.col('videoName')).distinct()
    dimVideoDF = dimVideoDF.withColumn("videoKey", F.monotonically_increasing_id() + 1)
    dimVideoDF.createOrReplaceTempView('dimVideoTable')
    # Creating fact helper DF including Video field which will help to create fact field in downstream
    factHelperVideoDF = spark.sql("Select a.rawKey, b.videoKey, b.videoName from \
                                         videoTable a JOIN \
                                         dimVideoTable b ON \
                                         a.videoName=b.videoName").distinct()
    return dimVideoDF, factHelperVideoDF


def videoStartMetric(deduped_rawData, dimDate, dimPlatform, dimSite, dimVideo):
    '''
    Creates fact table combining the raw and dimensional dataframes
    :param rawData: deduped source raw dataframe
    :param dimDate: date dimension dataframe
    :param dimPlatform: platform dimension dataframe
    :param dimSite: site dimension dataframe
    :param dimVideo: video dimension dataframe
    :return:
    '''
    # Caching the temp tables to perform better in joins
    deduped_rawData.createOrReplaceTempView('raw_table')
    spark.catalog.cacheTable('raw_table')
    dimDate.createOrReplaceTempView('dim_date')
    spark.catalog.cacheTable('dim_date')
    dimPlatform.createOrReplaceTempView('dim_platform')
    spark.catalog.cacheTable('dim_platform')
    dimSite.createOrReplaceTempView('dim_site')
    spark.catalog.cacheTable('dim_site')
    dimVideo.createOrReplaceTempView('dim_video')
    spark.catalog.cacheTable('dim_video')

    # Joining fact helper temp tables
    factTableDF = spark.sql("select b.dateKey,c.videoKey,d.platformKey,e.siteKey,b.dateMinute, a.events from raw_table a \
             LEFT OUTER JOIN \
             dim_date b \
             ON a.rawKey=b.rawKey \
             LEFT OUTER JOIN \
             dim_video c \
             ON a.rawKey=c.rawKey \
             LEFT OUTER JOIN \
             dim_platform d \
             ON a.rawKey=d.rawKey \
             LEFT OUTER JOIN \
             dim_site e \
             ON a.rawKey=e.rawKey \
             ORDER BY b.dateMinute ASC \
             " \
                            ).coalesce(1)

    # Caching the DF as it will called couple of times
    factTableDF.cache()
    factTableDF.count()
    # facts table
    factsTableDF = factTableDF.withColumn("video_events_count", F.size(
        F.split(F.col("events"), ","))).drop(
        F.col('events')).coalesce(1)
    # Counting the number of events associated without 206
    metricsWithout206DF = factTableDF.where(~F.col('events').like('%206%')).withColumn("video_events_count", F.size(
        F.split(F.col("events"), ","))).drop(
        F.col('events')).coalesce(1)

    # Counting the number of events associated with 206
    metricsWith206DF = factTableDF.where(F.col('events').like('%206%')).withColumn("video_events_count", F.size(
        F.split(F.col("events"), ","))).drop(
        F.col('events')).coalesce(1)
    return factsTableDF, metricsWithout206DF, metricsWith206DF


if __name__ == "__main__":
    logger = logging.getLogger()
    # Reading the raw data from local file system
    rawData = spark.read.parquet(
        'file:/home/abhi/eclipse-welcome/video-streaming-analytics/resources/*.parquet')
    # de-duplicating the raw data
    deduped_rawData = rawData.drop_duplicates().coalesce(1)

    # Assigning key to each row - It will be used in downstream dimension table generation
    deduped_rawData = deduped_rawData.withColumn("rawKey", F.monotonically_increasing_id() + 1)
    logger.info(deduped_rawData.show(10, False))
    deduped_rawData.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/deduped_rawData.parquet', mode='overwrite')
    # Creating date dimensional DF and fact helper DF
    dimDate, factHelperDateDF = createDimDate(deduped_rawData.select(F.col('DateTime'), F.col('rawKey')))
    logger.info(dimDate.show(10, False))
    dimDate.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/dimDate.parquet', mode='overwrite')
    # Creating platform dimensional DF and fact helper DF
    dimPlatform, factHelperPlatformDF = createDimPlatform(deduped_rawData.select(F.col('VideoTitle'), F.col('rawKey')))
    logger.info(dimPlatform.show(10, False))
    dimPlatform.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/dimPlatform.parquet', mode='overwrite')
    # Creating site dimensional DF and fact helper DF
    dimSite, factHelperSiteDF = createDimSite(deduped_rawData.select(F.col('VideoTitle'), F.col('rawKey')))
    logger.info(dimSite.show(10, False))
    dimSite.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/dimSite.parquet', mode='overwrite')
    # Creating video dimensional DF and fact helper DF
    dimVideo, factHelperVideo = createDimVideo(deduped_rawData.select(F.col('VideoTitle'), F.col('rawKey')))
    logger.info(dimVideo.show(10, False))
    dimVideo.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/dimVideo.parquet', mode='overwrite')
    # Creating facts table with and without events 206
    factsTableDF, metricsWithout206DF, metricsWith206DF = videoStartMetric(deduped_rawData, factHelperDateDF,
                                                                          factHelperPlatformDF, factHelperSiteDF,
                                                                          factHelperVideo)
    logger.info(factsTableDF.show(10, False))
    logger.info(metricsWithout206DF.show(10, False))
    logger.info(metricsWith206DF.show(10, False))
    # Writing facts table and other metrics 
    factsTableDF.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/factsTableDF.parquet', mode='overwrite')
    metricsWithout206DF.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/metricsWithout206DF.parquet', mode='overwrite')
    metricsWith206DF.coalesce(1).write.parquet(
        '/home/abhi/eclipse-welcome/video-streaming-analytics/output/metricsWith206DF.parquet', mode='overwrite')
