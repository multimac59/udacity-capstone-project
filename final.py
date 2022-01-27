import os
import pandas as pd
import datetime as dt
import seaborn as sns
import matplotlib.pyplot as plt
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, lit
from pyspark.sql.functions import dayofmonth, dayofweek, month, year, weekofyear, datediff
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
import requests
requests.packages.urllib3.disable_warnings()

def drop_columns(df, cols):
    '''
        Description: This function drops columns from spark dataframe
        Arguments:
            a. df: spark dataframe
            b. cols: columns array
        Return: spark dataframe
    '''
    df = df.drop(*cols)
    return df
        

def rename_columns(df, cols_dict):
    '''
        Description: Here columns of spark dataframe will be renamed as per datamodel
        Arguments:
            a. df: spark dataframe
            b. cols_dict: dictionary having key name as existing column name and vlaue name as new column name
        Return: spark dataframe
    '''
    for k, v in cols_dict.items():
        df = df.withColumnRenamed(k, v)
    return df
        
def change_datatype_of_cols(df, data_type_dict):
    '''
        Description: This function changes data types of columns as per data model but not date columns
        Arguments:
            a. df: spark dataframe
            b. data_type_dict: dictionary having column name as key and value as new data type
        Return: spark dataframe
    '''
    for k, v in data_type_dict.items():
        df = df.withColumn(k,df[k].cast(v))
    return df
        
def change_sas_date_to_iso_format(df , date_cols_list):
    '''
        Description:
        Arguments:
            a. df: spark dataframe
            b. date_cols_list: iso_dates_column_names
        Return: spark dataframe
    '''
    #(dt.datetime(1960, 1, 1).date() + dt.timedelta(20566.0)).isoformat()
    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    # convert arrival date into datetime object
    for date_col in date_cols_list:
        df = df.withColumn(date_col, get_datetime(df[date_col]))
    
    return df

def change_string_col_to_date_col(df, col, string_format):
    '''
        Description: change column values to dates
        Arguments:
            a. df: spark dataframe
            b. col: date column name
            c. string_format: current string format in which date column appears
        Return: spark dataframe
    '''
    get_dates = udf(lambda x: dt.datetime.strptime(x, string_format).strftime('%Y-%m-%d') if x else None)
    df = df.withColumn(col, get_dates(df[col]))    
    return df

def create_time_dim(df, output_path):
    '''
        Description: This creates time dimension from immigration dataframe
        Arguments:
            a. df: spark dataframe 
            b. output_path: destination path where time dimension to be saved
        Return: It returns spark dataframe
    '''
    # create initial time dataframe 
    time_df = df.select(['arrival_date']).union(df.select(['depart_date'])).distinct()
    time_df = time_df.withColumnRenamed('arrival_date', 'date')
    # expand df by adding other calendar columns
    time_df = time_df.withColumn('day', dayofmonth('date'))
    time_df = time_df.withColumn('week', weekofyear('date'))
    time_df = time_df.withColumn('month', month('date'))
    time_df = time_df.withColumn('year', year('date'))
    time_df = time_df.withColumn('weekday', dayofweek('date'))

    # create an id field in calendar df
    time_df = time_df.withColumn('id', monotonically_increasing_id())
    # write the time dimension to parquet file
    partition_columns = ['year', 'month', 'week']
    time_df.write.parquet(output_path + "time_dim", partitionBy=partition_columns,mode="overwrite")
    return time_df

def create_visa_dim(df, output_path):
    '''
        Description: It creates visa dimension from immigration dataframe
        Parameters:
            df: immigration spark dataframe
            output_path: target path where visa dataframe to be saved
            return: visa spark dataframe
    '''
    # create initial visa dataframe from immigration fact dataframe
    visa_df = df.select(col('visa'), col('visatype')).distinct()
    # change column name of visa
    visa_df = rename_columns(visa_df, {'visa':'visa_cat_id'})
    # add new column 
    visa_df = visa_df.withColumn('visa_cat', when((visa_df['visa_cat_id'] == 1), lit("Business"))
                                .when((visa_df['visa_cat_id'] == 2), lit("Pleasure"))
                                .when((visa_df['visa_cat_id']== 3), lit("Student")))
    visa_df.write.parquet(output_path + "visa_dim",mode="overwrite")
                                 
    return visa_df

def create_port_dim(input_path, spark, output_path):
    '''
        Description: It creates ports dimension and save it into parquet format 
        Arguments:
            a. input_path: input path of .csv file from where data is read
            b. spark: spark session object
            c. output_path: output path where spark dataframe is written in parquet format
    '''
    # read port csv file into spark dataframe
    port_df = spark.read.option("delimiter", ",").option("header", True).csv(input_path)
    # rename columns
    cols_dict = {'Code':'port_id', 'City':'city', 'State':'state'}
    port_df = rename_columns(port_df, cols_dict)
    port_df.write.parquet(output_path + 'port_dim', mode = 'overwrite')
    return port_df

def create_global_temp_dim(input_path_temp,input_path_country, spark, output_path):
    '''
        Description: It creates global temperature dimension and write it as parquet file
        Arguments:
            a. input_path: temperature file input path
            b. spark: spark session
            c. output_path: path where temperature to be written as parquet file
            return: temperature spark dataframe
    '''
    # read port csv file into spark dataframe
    temp_df = spark.read.option("delimiter", ",").option("header", True).csv(input_path_temp)
    # read country data into spark dataframe
    country_df = spark.read.option("delimiter", ",").option("header", True).csv(input_path_country)
    # create temporary views on both dataframes
    temp_df.createOrReplaceTempView("TEMP")
    country_df.createOrReplaceTempView("COUNTRY")
    # create final temperature dataframe having country id
    final_temp_df = spark.sql('''
                                    SELECT t.City city,c.Code  country_id,t.Country 
                                     country,t.average_temperature avg_temperature 
                                    FROM TEMP t INNER JOIN COUNTRY c ON lower(t.Country) == lower(c.Country)
                                ''')
    final_temp_df.write.parquet(output_path + 'temperature_dim', mode = 'overwrite')
    return final_temp_df

def create_transport_mode_dim(df, output_path):
    '''
        Description: It creates transport mode dimension from immigration dataframe
        Parameters:
            a. df: immigration spark dataframe
            b. output_path: target path where dataframe to be saved
        Return: transport_mode  spark dataframe
    '''
    # create initial transport mode dataframe from immigration fact dataframe
    trans_mod_df = df.select(col('transport_mode_id')).distinct()
    trans_mod_df = trans_mod_df.withColumnRenamed('transport_mode_id', 'id')
    
    # add new column 
    trans_mod_df = trans_mod_df.withColumn('mode', when((trans_mod_df['id'] == 1), lit("Air"))
                                          .when((trans_mod_df['id'] == 2), lit("Sea"))
                                          .when((trans_mod_df['id'] == 3), lit("Land"))
                                          .when((trans_mod_df['id'] == 9), lit("Other")))
    # write to file
    trans_mod_df.write.parquet(output_path + "transport_mode_dim",mode="overwrite")
                                 
    return trans_mod_df

def create_us_cities_demographics_dim(spark, input_path, output_path):
    '''
        Description: this function creates city demographics dimension.
        Arguments:
            a. spark: spark session object
            b. input path: dempgraphics file input path
            c. output_path: output path to save spark dataframe
        Return: demographics spark dataframe
    '''
     # read port csv file into spark dataframe
    city_df = spark.read.option("delimiter", ",").option("header", True).csv(input_path)
    
    if '_c0' in city_df.columns:
        city_df = city_df.drop('_c0')
    
    # rename columns
    cols_dict = {'City':'city', 'State':'state', 'State Code':'state_code', 'Median Age':'median_age', 
                 'Male Population':'male_population', 'Female Population':'female_population', 
                 'Total Population':'total_population', 'Number of Veterans':'number_of_veterans', 
                 'Foreign-born':'foreign_born', 'Average Household Size':'average_household_size',
                 'Race':'race', 'Count':'count'
                }
    
    city_df = rename_columns(city_df, cols_dict)
    
    # change datatypes of cols
    cols_dict = {'count':'int', 'median_age':'float', 'male_population':'bigint', 'female_population':'bigint',
                 'total_population':'bigint','number_of_veterans':'bigint', 'foreign_born':'bigint', 
                 'average_household_size':'float'
                }
    city_df = change_datatype_of_cols(city_df, cols_dict)
     
     # group by all columns except race and count, pivot on race and sum on count
    cols = city_df.columns
    cols = [col for col in cols if col not in ('race', 'count')]
    city_df = city_df.groupBy(cols).pivot("race").sum("count")
    
    # renaming race columns
    new_cols = city_df.columns
    race_cols = [col for col in new_cols if col not in cols]
    for col in race_cols:
        new_name = col.lower().replace(' ','_').replace('-','_')
        city_df = city_df.withColumnRenamed(col, new_name)
        
    print(city_df.count())
    city_df.printSchema()
    
    # write to file
    city_df.write.parquet(output_path + "us_city_demographics_dim",mode="overwrite")
    return city_df

def process_immigrant_stg_data(file_path, spark,output_path):
    '''
        Description: This functions creates immigration fact table or data from provided immigration data and save it into  S3 bucket final data path
        Arguments:
            1. file_name = immigration data file name
            2. spark = spark session
    '''
    df_immg = spark.read.parquet(file_path)
    
    # drop columns which are not required
    cols = ('i94yr', 'i94mon', 'visapost', 'entdepa', 'entdepd', 'matflag', 'admnum', 'fltno', 'dtadfile', 'dtaddto')
    df_immg = drop_columns(df_immg, cols)
    # changes column names
    cols_dict = {'cicid':'id', 'i94cit':'birth_country_id', 'i94res':'residence_country_id', 'i94port':'port_id',
                 'arrdate':'arrival_date', 'i94mode':'transport_mode_id', 'i94addr':'arrival_state', 
                 'depdate':'depart_date', 'i94bir':'age_in_yrs', 'i94visa':'visa', 'biryear':'birth_year'       
                }
    df_immg = rename_columns(df_immg, cols_dict)
    # change data types of columns
    cols_dict = {'id':'int', 'birth_country_id':'int', 'residence_country_id':'int', 
                 'port_id':'string', 'transport_mode_id':'smallint', 'arrival_state':'varchar(2)', 
                 'age_in_yrs':'int', 'visa':'smallint', 'count':'int', 'birth_year':'int', 
                 'gender':'varchar(1)', 'airline':'string', 'visatype':'string'
                }
    df_immg = change_datatype_of_cols(df_immg, cols_dict)
    # arrival_date, depart_date, stay_days (calculate), file_date, stay_till_date
    
    # change sas dates to iso dates
    cols_list = ['arrival_date', 'depart_date']
    df_immg = change_sas_date_to_iso_format(df_immg, cols_list)
    
    # add column stay_days between depart_date and arrival_date
    df_immg = df_immg.withColumn("stay_days", datediff(col("depart_date"),col("arrival_date")))
    
    #write to parquet
    df_immg.write.parquet(output_path + "immigration_fact",mode="overwrite")

    return df_immg
    
    
    

