## ETL PIPELINE WITH AMAZON REDSHIFT
#### INTRODUCTION
The purpose of this project is to create an ETL pipeline for the analysis of user activity of a music streaming app called sparkify.
Currently, they don't have an easy way to query their data, which are JSON logs saved in AWS s3 bucket.
This project creates an ETL pipeline to fetch the data and then creates a database in Redshitf with a star schema in which the data is organized and saved for analysis.

#### DATABASE SCHEMA
The *star schema* is used for the database with `songplays` as the **fact table** and `users, songs, artists and time` as **dimension tables**
This makes the querying of the data much easier for analysis.

### HOW TO RUN THIS PROJECT

#### Required Python Packages

 - *configparser*
 - *psycopg2*

> NOTE: Consider using a virtual envronment.

#### FILES

 - `create_tables.py :` contains code for dropping and creating database tables.
 - `etl.py` contains code to run the entire ETL process
 - `sql_queries.py`  contains the SQL queries need for this ETL project

#### STARTUP
Make sure you have all the required packages installed on your machine. Creat a config file `dwh.cfg` file to enter your configuration details as shown below:

```
[CLUSTER]
HOST=<your_host>
DB_NAME=<your_db_name>
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
DB_PORT=<your_db_port>
DB_REGION=<your_db_region>
CLUSTER_IDENTIFIER=<your_cluster_identifier>

[IAM_ROLE]
ARN=<your_iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
ACCESS_KEY=<your_access_key>
SECRET_KEY=<your_secret_key>
```

From the project directory run the following command:

    $ python3 create_tables.py
    $ python3 etl.py


## Query Example

After running the ETL pipeline, you can run some test
queries in Redshift console query editor:

```
--Number of song plays before Nov 15, 2018
select count(*) from songplays where start_time < '2018-11-15'
```

```
--Top artists by number of song plays
select a.name, count(a.name) as n_songplays
from songplays s
left join artists a on s.artist_id = a.artist_id
group by a.name
order by n_songplays desc
```