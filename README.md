# Data Lake with Apache Spark

Data lake and ETL pipeline in Apache Spark that loads data from Amazon S3 and processes the data into analytics tables which are loaded back to S3.

## Background
The analytics team for music streaming startup Sparkify wants to analyze the song-listening activity of their users. This analysis will be based on mobile app JSON user activity logs and song metadata that exists in Amazon S3.

## Datasets
The datasets are in a single Amazon S3 bucket, with song and log datasets in song_data and log_data folders, respectively.

### Song data
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
Below is an example of the content in a single song file.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

### Log data
Log files are partitioned by year and month.
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
This image below illustrates what the data in a log file looks like.

![log data](log-data.png)

## Analytics tables
The ETL pipeline will create a star schema of analytics tables.  Star schema models allow for simple queries and performance optimization for read-only analysis.

### Fact table
1. **songplays** - ecords in log data associated with song plays i.e. records with page `NextSong`

    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

### Dimension tables
2. **users**
    - *user_id, first_name, last_name, gender, level*
3. **songs**
    - *song_id, title, artist_id, year, duration*
4. **artists**
    - *artist_id, name, location, lattitude, longitude*
5. **time**
    - *tart_time, hour, day, week, month, year, weekday*

## Project files

- `etl.py` - Python script for running the ETL pipeline
- `README.md` - a summary of the project, how to run the Python scripts, and an explanation of the files in the repository
- `dl.cfg` - config file for AWS credentials

## Steps to run project

In `etl.py`, modify variable `output_data` to refer to your destination S3 bucket/folder.

### Local environment
1. Add AWS access key and secret key in `dl.cfg`
    - An account with read and write access on S3 is required
```
[AWS]
AWS_ACCESS_KEY_ID=YOURACCESSKEYGOESHERE
AWS_SECRET_ACCESS_KEY=PUTyourSECRETaccessKEYhereTHISisREQUIRED
```
2. Open command prompt

3. Execute ETL pipeline
```
python etl.py
```

### Amazon EMR
1. Create an EMR cluster with Cluster launch mode and configured for Spark applications
    - For security, choose an [EC2 key pair .pem file](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html)

2. Upload `etl.py` to an S3 bucket

3. [SSH into the EMR](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)

4. Download `etl.py` from S3
```
aws s3 cp s3://my-s3-bucket/etl.py
```
5. Run Spark job
```
spark-submit etl.py
```

