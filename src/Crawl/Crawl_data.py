import requests
import pandas as pd 
import time
import numpy as np
import random
import time
import datetime
import pyspark.sql.functions as sf
from uuid import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from uuid import * 
from pyspark.sql.window import Window as W
spark = SparkSession.builder \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0") \
    .config("spark.cassandra.connection.host", "my-cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()
column_name = ['ad_id', 'list_id', 'list_time', 'date', 'account_id', 'account_oid', 'account_name', 'state', 'subject', 'body', 'category', 'category_name', 'area', 'area_name', 'region', 'region_name', 'company_ad', 'type', 'price', 'price_string', 'image', 'webp_image', 'videos', 'number_of_images', 'avatar', 'rooms', 'property_legal_document', 'size', 'region_v2', 'area_v2', 'ward', 'ward_name', 'direction', 'toilets', 'floors', 'price_million_per_m2', 'house_type', 'furnishing_sell', 'width', 'length', 'contain_videos', 'location', 'longitude', 'latitude', 'phone_hidden', 'owner', 'street_number', 'protection_entitlement', 'escrow_can_deposit', 'params', 'zero_deposit', 'street_name', 'pty_jupiter', 'label_campaigns', 'ad_labels', 'streetnumber_display', 'living_size', 'pty_characteristics', 'detail_address', 'has_video', 'special_display', 'apartment_type', 'property_status', 'company_logo', 'land_type', 'projectid', 'project_oid', 'block', 'floornumber', 'balconydirection', 'unitnumber', 'projectimages', 'address', 'unitnumber_display', 'apartment_feature', 'commercial_type']
skip_col = ['params', 'pty_jupiter', 'label_campaigns', 'special_display_images', 'ad_labels', 'pty_characteristics','image', 'webp_image', 'videos', 'avatar',"zero_deposit","protection_entitlement","escrow_can_deposit"]
def extract_ad_data(ad):
    ad_data = {key: ad.get(key, None) for key in column_name if key not in skip_col}
    return ad_data
def crawl_data():
    region_dict = {
        'Quận 1' : 13096,
        'Quận 3' : 13098,
        'Quận 4' : 13099,
        'Quận 5' : 13100,
        'Quận 6' : 13101,
        'Quận 7' : 13102,
        'Quận 8' : 13103,
        'Quận 10' : 13105,
        'Quận 11' : 13106,
        'Quận 12' : 13107,
        'Quận Bình Tân' : 13108,
        'Quận Bình Thạnh' : 13109,
        'Quận Gò Vấp' : 13110,
        'Quận Phú Nhuận' : 13111,
        'Quận Tân Bình' : 13112,
        'Quận Tân Phú' : 13113,
        'Huyện Bình Chánh' : 13114,
        'Quận Cần Giờ' : 13115,
        'Quận Củ Chi' : 13116,
        'Quận Hóc Môn' : 13117,
        'Quận Nhà Bè' : 13118,
        'Thành phố Thủ Đức' : 13119
    }
    for keys,value in region_dict.items():
        user_agents = [ 
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36', 
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36', 
                'Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148', 
                'Mozilla/5.0 (Linux; Android 11; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.72 Mobile Safari/537.36' 
            ] 
        result_df = pd.DataFrame(columns=column_name)
        o=0
        page = 1
        limit = 20
        region = 13000
        area = value
        sam = f"https://gateway.chotot.com/v1/public/ad-listing?region_v2={region}&area_v2={area}&cg=1000&o={o}&page={page}&st=s,k&limit=5000&key_param_included=true"
        response = requests.get(sam)
        data = response.json()
        if len(data['ads']) == 0 : continue
        number = data['total']
        print(keys)
        print(number)
        while True : 
            headers = {'User-Agent': random.choice(user_agents)}
            sam = f"https://gateway.chotot.com/v1/public/ad-listing?region_v2={region}&area_v2={area}&cg=1000&o={o}&page={page}&st=s,k&limit={limit}&key_param_included=true"
            response = requests.get(sam,headers=headers)
            data = response.json()
            number =data['total']
            all_rows = []
            for ad in data['ads']:
                row_data = extract_ad_data(ad)
                all_rows.append(row_data)
            sub_df = pd.DataFrame(all_rows)
            if page == 1 : 
                result_df = sub_df
            else : result_df = pd.concat([result_df,sub_df],ignore_index=True,axis=0)
            if number < 200 : 
                if result_df.shape[0] >= number : break
            if result_df.shape[0] >= 200 : break
            page +=1
            o = o + limit
            time.sleep(5)
        if value == 13096:
            full_df=result_df
        else: full_df = pd.concat([full_df,result_df],ignore_index=True,axis=0)
    return full_df
def preprocessing(df):
    df.fillna(np.nan,inplace=True)
    df.dropna(subset=['list_time'], inplace=True)
    df['list_time'] = df['list_time'].astype(np.int64)
    df.dropna(subset=['category',"area","region","region_v2","area_v2","ward"], inplace=True)
    df[['category',"area","region","region_v2","area_v2","ward"]] = df[['category',"area","region","region_v2","area_v2","ward"]].astype(np.int64)
    df['special_display'].fillna(False,inplace=True)
    df['has_video'].fillna(False,inplace=True)
    return df
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'raw_house_price',keyspace = 'data_warehouse').load()
    cassandra_latest_time = data.agg({'list_time':'max'}).take(1)[0][0]
    return cassandra_latest_time
def write_cassDB(df):
    spark_df = spark.createDataFrame(df)
    spark_df = spark_df.withColumn('list_time', sf.date_format((sf.col('list_time') / 1000).cast('timestamp'), 'yyyy-MM-dd'))
    spark_df = spark_df.withColumn("list_time", sf.to_date("list_time", "yyyy-MM-dd"))
    cassandra_max_time = get_latest_time_cassandra()
    spark_df = spark_df.filter(sf.col('list_time') > cassandra_max_time)
    try:
        spark_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="raw_house_price", keyspace="data_warehouse") \
            .mode("append") \
            .save()
        print("DataFrame written to Cassandra successfully!")
    except Exception as e:
        print("Error writing DataFrame to Cassandra:", e)
def main_task():
    print("Starting calling API ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    raw_df = crawl_data()
    print("Finishing calling API ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("Starting preprocessing Pandas DataFrame ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    df = preprocessing(raw_df)
    print("Finishing preprocessing Pandas DataFrame ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("Starting converting into Spark DataFrame and writing to Cassandra DB ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    write_cassDB(df)
    print("Finishing converting into Spark DataFrame and writing to Cassandra DB ")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    print("----------------------------------------------------")
    return print("Done all tasks")



main_task()