import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

import requests
requests.packages.urllib3.disable_warnings()

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# import stage file and its functions. this file contains all logic of creating stage tables.
import stage
from stage import missing_values_spark_data_frame , clean_spark_dataframe
from stage import missing_values_pandas_dataframe, clean_pandas_df
from stage import create_immigration_stage_table, create_temperature_stage_table, create_us_cities_demographics_stage_table, create_ports_stage_table, create_country_stage_table


config = configparser.ConfigParser()
config.read('config.cfg', encoding='utf-8-sig')
#config.read('config.cfg')

# read AWS keys
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# following are the parent paths in S3 bucket for source, stage and final data
SOURCE_DATA_PATH = config['S3_PATHS']['SOURCE_DATA_PATH']
STG_DATA_PATH = config['S3_PATHS']['STG_DATA_PATH']
FINAL_DATA_PATH = config['S3_PATHS']['FINAL_DATA_PATH']

def get_spark_session():
    '''
        Description: This function creates spark session.
        Return: spark session
    '''
    
    print(os.environ['AWS_SECRET_ACCESS_KEY'])
    print(os.environ['AWS_ACCESS_KEY_ID'])
    
    spark = SparkSession \
    .builder \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
    .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
    .getOrCreate()
    
    '''
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])\
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])\
        .enableHiveSupport().getOrCreate()
    '''
    
    '''
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    fs.s3a.access.key
    fs.s3a.secret.key
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])\
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])\
    '''
    
    return spark


    
def main():
    spark = get_spark_session()
    print(spark)
    #create immigration staging data
    immigration_file_path = SOURCE_DATA_PATH + "data/18-83510-I94-Data-2016/" + "i94_apr16_sub.sas7bdat"
    final_output_path = STG_DATA_PATH + "sas_data"
    stage.create_immigration_stage_table(immigration_file_path, spark, final_output_path)
    
    # create temperature staging data
    temperature_file_name = "GlobalLandTemperaturesByCity.csv"
    temp_file_path = SOURCE_DATA_PATH + "data2/" + temperature_file_name
    final_output_path = STG_DATA_PATH + "global_temperatures"
    stage.create_temperature_stage_table(temp_file_path, spark, final_output_path)
    
    # create us cities demographics staging data
    usa_demographics_file_name = "us-cities-demographics.csv"
    input_file_path = SOURCE_DATA_PATH + usa_demographics_file_name
    output_file_path = STG_DATA_PATH + "us-cities-demographics.csv"
    stage.create_us_cities_demographics_stage_table(input_file_path, output_file_path)
    
    # create ports staging data
    port_file_name = "i94_ports.csv"
    input_file_path = SOURCE_DATA_PATH + port_file_name
    output_file_path = STG_DATA_PATH + port_file_name
    stage.create_ports_stage_table(input_file_path, output_file_path)
    
    # create countries staging data
    countries_file_name = "i94_countries.csv"
    input_file_path = SOURCE_DATA_PATH + countries_file_name
    output_file_path = STG_DATA_PATH + countries_file_name
    stage.create_country_stage_table(input_file_path, output_file_path) 


if __name__ == "__main__":
    main()