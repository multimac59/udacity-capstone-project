import pandas as pd
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

import requests
requests.packages.urllib3.disable_warnings()

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# final is the .py file which contains logics to create fact and dimensions. Following is imported from final
import final
from final import drop_columns, rename_columns , change_datatype_of_cols
from final import change_sas_date_to_iso_format , change_string_col_to_date_col
from final import process_immigrant_stg_data, create_time_dim, create_visa_dim, create_transport_mode_dim, create_global_temp_dim, create_port_dim, create_us_cities_demographics_dim


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

# following are the parent paths in S3 bucket for stage and final data
STG_DATA_PATH = config['S3_PATHS']['STG_DATA_PATH']
FINAL_DATA_PATH = config['S3_PATHS']['FINAL_DATA_PATH']

def get_spark_session():
    '''
        Description: This function creates spark session.
        Return: spark session
    '''
    spark = SparkSession.builder.\
    config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0").\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    return spark


def print_schema_n_rows(df_list):
    for df in df_list:
        df.printSchema()
        df.show(10)
 
    
def main():
    spark = get_spark_session()

    #create immigration fact
    input_path = STG_DATA_PATH + "sas_data"
    output_path = FINAL_DATA_PATH 
    df_immg = final.process_immigrant_stg_data(input_path, spark, output_path)
    
    # create time dimension 
    time_df = final.create_time_dim(df_immg, output_path)
    
    #create visa dimension
    visa_df = final.create_visa_dim(df_immg, output_path)
    
    # create transport mode dim
    trans_mod_df = final.create_transport_mode_dim(df_immg, output_path)
    
    # create temperature dimension
    input_path = STG_DATA_PATH + "global_temperatures"
    input_path_country = STG_DATA_PATH + "i94_countries.csv"
    df_temp_dim = final.create_global_temp_dim(input_path, input_path_country, spark, output_path)
     
    # create us cities demographics dimension
    input_path = STG_DATA_PATH + "us-cities-demographics.csv"
    df_us_city_dim = final.create_us_cities_demographics_dim(spark, input_path, output_path)
    
    # create port dim
    input_path = STG_DATA_PATH + "i94_ports.csv"
    df_port_dim = final.create_port_dim(input_path,spark, output_path)
    
     


if __name__ == "__main__":
    main()