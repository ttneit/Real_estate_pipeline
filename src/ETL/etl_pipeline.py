import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit,round
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
    .getOrCreate()
def preprocessing(df) :
    new_df = df.filter(~sf.isnan('latitude') & (~sf.isnan('longitude')))
    new_df = new_df.withColumn("rooms", sf.when((sf.isnan("rooms")) & (col('category') == 1040), 0).otherwise(col("rooms")))
    new_df = new_df.withColumn("rooms", sf.when((sf.isnan("rooms")) & (col('category') == 1030), 0).otherwise(col("rooms")))
    new_df = new_df.withColumn("price", round(new_df["price"]/1e9, 3)) \
            .withColumn("price_million_per_m2", round(new_df["price_million_per_m2"], 2)) \
            .withColumn("size", round(new_df["size"], 2))
    new_df = new_df.withColumn('sources',lit('NhaTot'))
    return new_df
def calculating_analysis(df):
    cate_output = (
        df
        .groupBy(
            'region_v2',
            'area_v2',
            'ward',
            'list_time',
            'category'
        )
        .agg(
            sf.avg('price').alias('average_price'),
            sf.avg('price_million_per_m2').alias('average_price_per_m2'),
            sf.avg('size').alias('average_size'),
            sf.count('ad_id').alias('number_list')
        )
        .orderBy(sf.desc('list_time'))
    )
    cate_output = cate_output.withColumn("average_price", round(cate_output["average_price"], 2)) \
               .withColumn("average_price_per_m2", round(cate_output["average_price_per_m2"], 2)) \
               .withColumn("average_size", round(cate_output["average_size"], 2))

    return cate_output 
# def process_cassandra_data(df):
#     final_output = calculating_category(df)
#     return final_output
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'raw_house_price',keyspace = 'data_warehouse').load()
    cassandra_latest_time = data.agg({'list_time':'max'}).take(1)[0][0]

    return cassandra_latest_time.strftime('%Y-%m-%d')
def get_mysql_latest_time():    
    sql = """(select max(list_time) from process_house_price_v2) data"""
    mysql_time = spark.read.format('jdbc').options(url=URL, driver=DRIVER, dbtable=sql, user=USER, password=PASSWORD).load()
    mysql_time = mysql_time.take(1)[0][0]
    mysql_time = mysql_time.strftime('%Y-%m-%d')
    return mysql_time

def import_to_mysql(output):
    output = output.withColumn('sources',lit('NhaTot'))
    output.write.format("jdbc") \
        .option("driver",DRIVER) \
        .option("url", URL) \
        .option("dbtable", "process_house_price_v2") \
        .mode("append") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .save()
    return print('Data imported MMySQL successfully')
def import_to_sql_servver(df,table_name):
    url = "jdbc:sqlserver://DESKTOP-301A075\DATA_WAREHOUSE:1433;databaseName=learning"
    properties = {
        "user": "sa",
        "password": "tien",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    df.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)

    return print('Data imported SQL server successfully')

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
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="raw_house_price",keyspace="data_warehouse").load()
    df = df.select('ad_id','region_v2','area_v2','ward','list_time','category','price','price_million_per_m2','size','latitude','longitude','rooms').where(col('list_time')>= mysql_time)
    print('-----------------------------')
    df.printSchema()
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    processed_df = preprocessing(df)
    cassandra_output = calculating_analysis(processed_df)
    # info_df = read_infor_df()
    # final_output = cassandra_output.join(info_df,on=['area_v2', 'ward'],how='left').drop(info_df.area_v2).drop(info_df.ward)
    processed_df = processed_df.dropDuplicates()
    processed_df.show(truncate=False)
    cassandra_output = cassandra_output.dropDuplicates()
    cassandra_output.show(truncate=False)
    print('-----------------------------')
    # print('Import Output to MySQL')
    # print('-----------------------------')
    # # import_to_mysql(final_output)
    print('-----------------------------')
    print('Import Output to SQL Server')
    print('-----------------------------')
    import_to_sql_servver(processed_df,"house")
    import_to_sql_servver(cassandra_output,"house_analysis")
    return print('Task Finished')

if __name__ == '__main__' :
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        main_task(mysql_time)
    else :
        print("No new data found")
    main_task(mysql_time)
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    