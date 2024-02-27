import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from uuid import * 
from uuid import UUID
from pyspark.sql.window import Window as W
import datetime
import time

HOST = 'my-mysql'
PORT = '3306'
DB_NAME = 'Data_Warehouse'
USER  = 'root'
PASSWORD = '1'
URL  = 'jdbc:mysql://' + HOST + ':' + PORT + '/' + DB_NAME
DRIVER = "com.mysql.cj.jdbc.Driver"
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
    .config("spark.cassandra.connection.host", "my-cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config('spark.jars',  "..\\MySQL_Connector\\mysql-connector-java-8.0.30.jar") \
    .getOrCreate()
def calculating_category(df):
    cate_output = (
        df
        .groupBy(
            'region_v2',
            'area_v2',
            'ward',
            'list_time',
            'category_name'
        )
        .agg(
            sf.avg('price').alias('average_price'),
            sf.avg('price_million_per_m2').alias('average_price_million_per_m2'),
            sf.avg('size').alias('average_size'),
            sf.count('*').alias('number_list')
        )
        .orderBy(sf.desc('list_time'))
    )
    return cate_output 
def process_cassandra_data(df):
    final_output = calculating_category(df)
    return final_output
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'raw_house_price',keyspace = 'data_warehouse').load()
    cassandra_latest_time = data.agg({'list_time':'max'}).take(1)[0][0]

    return cassandra_latest_time.strftime('%Y-%m-%d')
def get_mysql_latest_time():    
    sql = """(select max(list_time) from house_price) data"""
    mysql_time = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    mysql_time = mysql_time.take(1)[0][0]
    mysql_time = mysql_time / 1000
    mysql_time = datetime.datetime.fromtimestamp(mysql_time)
    if mysql_time is None:
        mysql_latest = '1998-01-01'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d')
    return mysql_latest 

def import_to_mysql(output):
    output = output.withColumn('sources',lit('NhaTot'))
    output.write.format("jdbc") \
        .option("driver",DRIVER) \
        .option("url", URL) \
        .option("dbtable", "process_house_price") \
        .mode("append") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .save()
    return print('Data imported successfully')

def read_infor_df():
    sql = """(select * from info_df) data"""
    info_df = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    return info_df

def main_task(mysql_time):
    print('The host is ' ,HOST)
    print('The port using is ',PORT)
    print('The db using is ',DB_NAME)
    print('-----------------------------')
    print('Retrieving data from Cassandra')
    print('-----------------------------')
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="raw_house_price",keyspace="data_warehouse").load().where(col('list_time')>= mysql_time)
    print('-----------------------------')
    df.printSchema()
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(df)
    info_df = read_infor_df()
    final_output = cassandra_output.join(info_df,on=['area_v2', 'ward'],how='left').drop(info_df.area_v2).drop(info_df.ward)
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output)
    return print('Task Finished')

while True :
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        main_task(mysql_time)
    else :
        print("No new data found")
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)