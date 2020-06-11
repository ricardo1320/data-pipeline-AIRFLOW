#Imports
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime

import fetching_tweet
import cleaning_tweet

default_args = {
    "start_date": datetime(2020,01,01),
    "owner": "airflow"
}

#Create a dag object with the "with" Python operator.
with DAG(dag_id="twitter_dag", schedule_interval="@daily",default_args=default_args, catchup=False) as dag:
    #Tasks:
    #Task of type SENSOR. This task will verify every 5 seconds if the file data.cvs is in the given directory or not.
    waiting_for_tweets = FileSensor(task_id="waiting_for_tweets",fs_conn_id="fs_tweet",filepath="data.csv",poke_interval=5)
    #Task of type ACTION. This task will execute a Python function.
    fetching_tweets = PythonOperator(task_id="fetching_tweets",python_callable=fetching_tweet.main)
    cleaning_tweets = PythonOperator(task_id="cleaning_tweets",python_callable=cleaning_tweet.main)
    #Task of type ACTION. This task will execute a bash command.
    storing_tweets = BashOperator(task_id="storing_tweets",bash_command="hadoop fs -put -f /tmp/data_cleaned.csv /tmp/")
    #Task of type TRANSFER. This task will execute a Hive command.
    loading_tweets = HiveOperator(task_id="loading_tweets",hql="LOAD DATA INPATH '/tmp/data_cleaned.csv' INTO TABLE tweets")

    #Define the dependencies between the tasks:
    waiting_for_tweets >> fetching_tweets >> cleaning_tweets >> storing_tweets >> loading_tweets

#Create a HIVE table to store the tweets from the file: data_cleaned.csv, that is already in HDFS.
#CREATE TABLE IF NOT EXISTS tweets (
#    tweet String,
#    dt Date,
#    retweet_from String,
#    before_clean_len int,
#    after_clean_len int)
#COMMENT 'tweets data'
#ROW FORMAT DELIMITED
#FIELDS TERMINATED BY '\t'
#LINES TERMINATED BY '\n'
#STORED AS TEXTFILE;
