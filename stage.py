import os
import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType

import requests
requests.packages.urllib3.disable_warnings()

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def missing_values_spark_data_frame(df):
    '''
        Description: This will return pandas dataframe having columns and corresponding percentage of
        missing values against total colunt of spark dataframe passed as argument.
        
        Arguments:
        df: spark dataframe
        
        Returns: pandas dataframe having columns names and their corresponding missing values in percentage.
    '''
    # create a dataframe with missing values count per column
    nan_count_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) 
                                    for c in df.columns]).toPandas()

    # convert dataframe from wide format to long format
    nan_count_df = pd.melt(nan_count_df, var_name='cols', value_name='values')

    # count total records in df
    total = df.count()

    # now lets add % missing values column
    nan_count_df['% missing values'] = 100*nan_count_df['values']/total
    print(nan_count_df)
    
    #barplot showing missing values percentage for each column
    plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    bar_plot = sns.barplot(x='cols', y='% missing values', data=nan_count_df)
    plt.xticks(rotation=45)
    plt.show()
    
    return nan_count_df

def clean_spark_dataframe(nan_count_df, df, missing_range = 90):
    '''
        description: drop columns from spark dataframe having missing or null values greater than 
        passed missing range
        
        Arguments:
        nan_count_df: pandas dataframe having percentage of missing values for each column.
        df: spark dataframe
        missing_range: integer value used in finding columns list having missing values greater than
                        or equal to this argument.
    '''
    #checking those columns which have missing values greater than argument value missing_range
    missing_values_cols_list = nan_count_df[nan_count_df['% missing values'] >= missing_range]['cols'].to_list()
    #drop columns which have missing percentage greater than missing range
    df = df.drop(*missing_values_cols_list)
    # drop rows having null values
    df = df.dropna(how='all')
    print("Number of records after dropping empty rows: ", df.count())
    return df

def missing_values_pandas_dataframe(df):
    '''
        Description: This will return pandas dataframe having columns and corresponding percentage of
        missing values against total count of pandas dataframe passed as argument.
        
        Arguments:
        df: spark dataframe
        
        Returns: pandas dataframe having columns names and their corresponding missing values in  percentage.
    '''
    # View columns with missing data
    nan_count_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
    nan_count_df = nan_count_df.reset_index()
    nan_count_df.columns = ['cols', 'values']

    # calculate % missing values percentage
    nan_count_df['% missing values'] = 100*nan_count_df['values']/df.shape[0]
    
    #barplot showing missing values percentage for each column
    plt.rcParams["figure.figsize"] = [7.50, 3.50]
    plt.rcParams["figure.autolayout"] = True
    bar_plot = sns.barplot(x='cols', y='% missing values', data=nan_count_df)
    plt.xticks(rotation=45)
    bar_plot.set_ylim(0, 100)
    plt.show()
    
    return nan_count_df


def clean_pandas_df(nan_count_df, df, missing_range = 90, cols = []):
    '''
        Description: This functions cleans the pandas dataframe by dropping large missing values columns, 
        droppping rows having null values and dropping rows having subset of some particular columns null.
        
        Arguments:
            a. nan_count_df: pandas dataframe which contains column names and their missing values in percent of
                            another pandas dataframe argument df
            b. df: pandas dataframe that needs to be cleaned
            c. missing_range: integer value which is used as threshold for missing values of columns. Default is
                                90.
            d. cols: array of some important columns which are used to drop rows if row has missing values for
                     these columns
    '''
    
    # checking those columns which have missing values greater than argument value missing_range
    missing_values_cols_list = nan_count_df[nan_count_df['% missing values'] >= missing_range]['cols'].to_list()
    print(len(missing_values_cols_list))
    #drop columns which have missing percentage greater than missing_range
    if len(missing_values_cols_list) > 0:
        df = df.drop(missing_values_cols_list, axis = 1)
    
    #drop entire rows for all columns or list of columns where value is missing for all rows 
    df = df.dropna(how = 'all')
    if len(cols) != 0:
        df = df.dropna(subset = cols)
        
    # drop duplicated
    df = df.drop_duplicates()
    if len(cols) != 0:
        df = df.drop_duplicates(subset = cols)
        
    print("Number of records after dropping empty rows: ", df.count())
    return df



def create_immigration_stage_table(file_path, spark,final_output_path):
    '''
        Description: This function creates staging table or data from provided immigration data and save it into 
        S3 bucket staging data path
        Arguments:
            1. file_path = immigration data file path which is used to create immigration stage table
            2. spark = spark session
            3. output_path = stage immigration data is saved at this path
    '''
    print("reading immigration file from file path " + file_path)
    
    df_spark = spark.read.format('com.github.saurfang.sas.spark').load("s3a://mmittamm-capstone-project/raw_immigration_data/data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat")
    print("immigration file is read successfully")
    # Check missing and null values in immigration spark dataframe df_spark
    nan_count_df = missing_values_spark_data_frame(df_spark)
    # clean immigration spark dataframe
    df_spark = clean_spark_dataframe(nan_count_df, df_spark, 90)
    # dropping records for cicid duplicates
    df_spark.drop_duplicates(subset=['cicid'])
    df_spark.printSchema()
    print(df_spark.count)
    #write to parquet
    print("reading immigration staging")
    df_spark.write.parquet(final_output_path,mode="overwrite")
    print("immigration staging is written successfully at " + final_output_path)
    
def create_temperature_stage_table(file_path, spark, final_output_path):
    '''
        Description: This function creates temperature staging data from provided temperature data and save it
        into staging data path in .csv format
        Arguments:
            1. file_path = temperature data file path which is used to create temperature stage table
            2. spark = spark session
            3. output_path = stage temperature data is saved at this path
    '''
    
    df_spark = spark.read.option("delimiter", ",").option("header", True).csv(file_path)
    # Check missing and null values in temperature spark dataframe df_spark
    nan_count_df = missing_values_spark_data_frame(df_spark)
    # clean temperature spark dataframe
    df_spark = clean_spark_dataframe(nan_count_df, df_spark, 90)
    df_spark = df_spark.dropna(subset = ['AverageTemperature'])
    # drop duplicate records records where dt, city and country are same
    df_spark = df_spark.drop_duplicates(subset = ['dt', 'City', 'Country'])
    df_spark = df_spark.select(['City','Country','AverageTemperature']).groupby(['City', 'Country']).agg({'AverageTemperature':'avg'})
    df_spark = df_spark.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    # this dataframe can be saved to .csv file as it has less number of rows
    df = df_spark.toPandas()
    print(df.info())
    df.to_csv(final_output_path, sep=',', encoding='utf-8')
    
def create_us_cities_demographics_stage_table(input_file_path, output_file_path):
    '''
        Description: This function creates US cities demographics staging data.
        Arguments:
            1. file_path = US cities data file path which is used to create stage table
            2. output_path = stage US cities data is saved at this path
    '''
    
    df_cities = pd.read_csv(input_file_path, delimiter = ';')
    # percentage of missing values for columns
    nan_count_df = missing_values_pandas_dataframe(df_cities)
    
    # drop rows where rows are duplicates for all columns or certain columns
    cols = ['City' , 'State', 'State Code','Race']
    df_cities = clean_pandas_df(nan_count_df, df_cities,90, cols = cols)
    df_cities.to_csv(output_file_path, sep = ',', encoding = 'utf-8')
    
def create_ports_stage_table(input_file_path, output_file_path):
    '''
        Description: This function creates ports staging data.
        Arguments:
            1. input_file_path = Ports data file path which is used to create stage table
            2. output_file_path = Ports stage data is saved at this path
    '''
    
    df_ports = pd.read_csv(input_file_path, sep = ',')
    # percentage of missing values for columns
    nan_count_df = missing_values_pandas_dataframe(df_ports)
    print(nan_count_df)
    df_ports = clean_pandas_df(nan_count_df, df_ports)
    df_ports.to_csv(output_file_path, sep = ',', encoding = 'utf-8')
    
def create_country_stage_table(input_file_path, output_file_path):
    '''
        Description: This function creates countries staging data.
        Arguments:
            1. input_file_path = Countries data file path which is used to create stage table
            2. output_file_path = Countries stage data is saved at this path
    '''
    df_country = pd.read_csv(input_file_path, sep = ',')
    # percentage of missing values for columns
    nan_count_df = missing_values_pandas_dataframe(df_country)
    print(nan_count_df)
    df_country = clean_pandas_df(nan_count_df, df_country)
    df_country.to_csv(output_file_path, sep = ',', encoding = 'utf-8')


