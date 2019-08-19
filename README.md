# Data_Lake_with_Apache_Spark
In this project, I built an ETL pipeline for creating data lake hosted on S3. The ETL script loads data stored in JSON format in S3 using Spark, processes the data by doing necessary transformations and loads it into analytics tables serving as facts and dimensions tables using Spark. It saves the data frames into S3 in Parquet format to preserve schema of tables. The Parquet files in S3 are partitioned by appropriate attributes like year and month to facilitate quick retrieval of subset of data in the tables for future analysis by analytics and management teams.

## Analytics Requirement
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Overview

This Project handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

* **s3://udacity-dend/song_data**: static data about artists and songs
  Song-data example:
  `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

* **s3://udacity-dend/log_data**: event data of service usage e.g. who listened what song, when, where, and with which client
  ![Log-data example (log-data/2018/11/2018-11-12-events.json.png)


### Purpose of the database and ETL pipeline

In context of Sparkify, this Data Lake based ETL solution provides very elastic way of processing data. As pros, we can mention the following:

* Collecting input data to AWS S3, process the data as needed, and write it back to S3 without maintaining a separate database for intermediate or final data.
* This solution is most probably also less expensive as alternative database solutions like PostgresSQL DB or Redshift since AWS processing resources are needed only during the data processing, not for collecting or storing the data.
* Data processing is also fast, since Spark executes data processing in RAM and as a cluster which offers parallel processing capabilities.
* Spark creates schema on-the-fly, so separate schema design is not necessary needed.

However, as cons we could mention the following:

* Intensive RAM need on Spark cluster nodes.
* Also, data loading from S3 and to S3 may take more time than in alternative solutions utilising DB solutions since network connection is the slowest part of the ETL pipeline (compared to accessing data from RAM or processing data in CPU).
* In addition, S3 seems to be (as an object store) sub-optimal for storing Spark parquet type of files that might be very small and lots of them. So, Spark DataFrame partitioning needs careful attention.
* AWS account type also impacts S3 performance and throughput, which in turn impacts ETL pipeline script performance. E.g. if using AWS Education account type, S3 performance and hence ETL script performance may be very poor.
* One suggestions among Spark users seems to be read input data from S3, process it in Spark, write output data to parquet file locally, and upload generated parquet files to S3 as a patch procedure.


## Solution
### Database Schema
I decided to model the database as star schema, which looks like the diagram shown below.
![ER Diagram of Database Schema](.png)

The `songplays` table is a facts table that contains information about the user activity of playing specific song at specific time. It references to other dimension tables like `users`, `songs`, `artists` and `time` through their IDs stored in `songplays` table as foreign keys.

### ETL pipeline
The datasets songs and log data about user activity stored in S3 as JSON files under separate directories are extracted and loaded in to Spark using its JSON reading capability. Spark is able to detect the schema of the datasets and read the data files into objects of class `pyspark.sql.DataFrame`. These data frames are able to carry out various SQL like operations on the underlying data stored in Spark cluster.

The dimension tables for `artists` and `songs` are created from the data frame in Spark created from songs dataset by selecting appropriate columns and renaming column names as appropriate to match the schema. Similarly the `users` and `time` tables are created from the data frame in Spark associated with user activity log dataset.

The timestamp attribute in the table of log dataset indicating when each activity was performed by user was originally present in milliseconds format in the table. The attribute is transformed into Python's `datetime` and added as new column `start_time` in the `songplays` table.

The `time` table is created from the timestamps of the user activity of playing next song and by extracting various attributes from the `datetime` derived from the timestamp i.e. hour, month, day of week, week of year, year etc.

The `songplays` facts table is created from the `songs` dimension table and `logs` table, created from original datasets, after loading them as temporary view tables in Spark and performing JOIN operation over attributes of the song i.e. ID, song's length and artist's name matching in both the tables. 

The prepared tables are saved back to S3 in Parquet format. The Parquet format allows to save the data frame in persistent storage while preserving the desired schema to be retrieved upon the files being read in future. Each table is appropriately partitioned based on specific attributes and saved across multiple directories in S3 to speed up the access of underlying data in future by analytics teams, in case they are only interested in loading and analysing small subset of the entire table based on some specific value of the attribute used for partition. 

Specifically, `songplays` table files are partitioned by `year` and `month` attributes when the user's activity was logged. `songs` table files are partitioned by `year` of song's release date and `artist_id` of artist who created the song. `time` table files are partitioned by `year` and `month` corresponding to the timestamp of activity when user starts playing new song.
 

### How to run

* Create an AWS S3 bucket.
* Edit dl.cfg: add your S3 bucket name.
* Copy **log_data** and **song_data** folders to your own S3 bucket.
* Create **output_data** folder in your S3 bucket.

After installing python3 + Apache Spark (pyspark) libraries and dependencies, run from command line:

* `python3 etl.py` (to process all the input JSON data into Spark parquet files.)

---

## Summary

Project-4 provides customer startup Sparkify tools to analyse their service data in a flexible way and help them answer their key business questions like "Who listened which song and when?"

