import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Process song data files.  Write songs and artists tables to parquet files.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print("----- reading song data files -----")
    df = spark.read.json(song_data)

    # extract columns to create songs table
    song_fields = ["song_id", "title", "artist_id","year", "duration"]
    songs_table = df.selectExpr(song_fields).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print("----- writing songs table parquet files -----")
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artist_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
    artists_table = df.selectExpr(artist_fields).dropDuplicates()
    
    # write artists table to parquet files
    print("----- writing artists table parquet files -----")
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')

    
def process_log_data(spark, input_data, output_data):
    """
        Process log data files.  Write users, time, and songplays tables to parquet files.
    """    
    
    # get filepath to log data file  
    log_data = input_data + 'log_data/*/*/*.json'
    
    # read log data file
    print("----- reading log data files -----")
    df = spark.read.json(log_data)
    
    # filter by actions for song plays    
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table    
    user_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(user_fields).dropDuplicates()
    
    # write users table to parquet files
    print("----- writing users table parquet files -----")
    users_table.write.mode('overwrite').parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))    
    
    time_fields = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_table = df.selectExpr(time_fields).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print("----- writing time table parquet files -----")
    time_table.write.mode('overwrite').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs").drop("year") 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = (
        df.withColumn("songplay_id", monotonically_increasing_id())
          .join(song_df, song_df.title == df.song)
          .select(
              col("songplay_id"),
              col("timestamp").alias("start_time"),
              col("userId").alias("user_id"),
              col("level"),
              col("song_id"),
              col("artist_id"),
              col("sessionId").alias("session_id"),
              col("location"),
              col("userAgent").alias("user_agent"),
              col("month"),
              col("year")
          ).repartition("year", "month")
    )
        
    # write songplays table to parquet files partitioned by year and month
    print("----- writing songplays table parquet files -----")
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays/')

                                               
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://postcore.net/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()