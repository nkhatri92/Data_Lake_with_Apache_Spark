import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
"""
    This procedure extracts metadata of songs stored in S3 in JSON format. 
    It prepares data frames for artists and songs table from this dataset by selecting required columns and dropping duplicates.
    It saves these tables back to S3 in Parquet file format.
    Songs table is partitioned by year of song and artist ID while storing as Parquet file.
    Inputs:
    spark instance of Spark context
    input_data file path of directory where datasets for songs and user activity log data are stored
    output_data file path of directory where the dimensions tables are to be stored as Parquet format
"""

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =df[['song_id','title','artist_id','year','duration']].dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(os.path.join(output_data, "songs"))   

    # extract columns to create artists table
    artists_table = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    #artists_table = artists_table.write.parquet(os.path.join(output_data, "artists")).mode("overwrite")
    artists_table.write.parquet(output_data + "artists.parquet", mode="overwrite")
    
"""
    This procedure extracts data about user activity logs on music app stored in S3 in JSON format. 
    It prepares data frames for users, songplays and corresponding timestamps table from this dataset.
    It saves these tables back to S3 in Parquet file format.
    Songplays and timestamp tables are partitioned by year and month while storing as Parquet files.
    
    Inputs:
    Spark instance of Spark context
    input_data file path of directory where datasets for songs and user activity log data are stored
    output_data file path of directory where the dimensions tables are to be stored in Parquet format
"""


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays  
    df = df[ df.page == 'Next Song' ]

    # extract columns for users table      
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
    users_table = users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(df.ts))          
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp( (int(x)/1000.0) ), T.TimestampType()) #incase of error change to string
    df = df.withColumn("date_time", get_datetime(df.ts))
    
    # extract columns to create time table #change timestamp to date_time
    time_table = df.selectExpr("timestamp as start_time", "hour(timestamp) as hour", "day(timestamp) as day", "weekofyear(timestamp) as week", "month(timestamp) as month", "year(timestamp) as year", "dayofweek(timestamp) as weekday").dropDuplicates(["start_time"])
    
    # write time table to parquet files partitioned by year and month
    time_table =  time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "time")) 

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("song")
    df.createOrReplaceTempView("log")
    songplays_table = spark.sql("""
                SELECT  
                l.timestamp as start_time, 
                l.userId as user_id, 
                l.level as level, 
                s.song_id as song_id, 
                s.artist_id as artist_id, 
                l.sessionId as session_id,
                l.location  as location, 
                l.userAgent as user_agent,
                year(l.timestamp) as year,
                month(l.timestamp) as month
            FROM song s JOIN log l ON (l.song=s.title AND l.length=s.duration AND l.artist=s.artist_name)
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(os.path.join(output_data, "songplays")) 


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake.sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
