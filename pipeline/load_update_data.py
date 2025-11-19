
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *
import sys
import os


def stop_spark():
  if spark is not NONE:
    spark.stop()


def start_session_spark():

  try:
    spark = SparkSession.builder \
      .appName("Batch Log Processing with Iceberg") \
      .config("spark.ui.port", "4041") \
      .config(
          "spark.jars",
          ",".join([
              "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar",
              "/opt/spark/jars/hadoop-aws-3.2.2.jar",
              "/opt/spark/jars/aws-java-sdk-1.11.375.jar",
              "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
              "/opt/spark/jars/postgresql-42.5.0.jar",
              "/opt/spark/jars/commons-pool2-2.11.1.jar"
          ])
      ) \
      .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
      .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
      .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://warehouse/logs") \
      .config("spark.sql.catalog.hadoop_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO") \
      .config("spark.sql.catalog.hadoop_catalog.s3.endpoint", "http://minio:9000") \
      .config("spark.sql.catalog.hadoop_catalog.s3.access.key.id", "admin") \
      .config("spark.sql.catalog.hadoop_catalog.s3.secret.access.key", "password") \
      .config("spark.sql.catalog.hadoop_catalog.s3.region", "us-east-1") \
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
      .config("spark.hadoop.fs.s3a.access.key", "admin") \
      .config("spark.hadoop.fs.s3a.secret.key", "password") \
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
      .config("spark.hadoop.fs.s3a.path.style.access", "true") \
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
      .getOrCreate()
  except Exception as e:
    spark = SparkSession.builder \
        .appName("Batch Log Processing with Iceberg") \
        .config("spark.ui.port", "4041") \
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar",
                "/opt/spark/jars/hadoop-aws-3.2.2.jar",
                "/opt/spark/jars/aws-java-sdk-1.11.375.jar",
                "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
                "/opt/spark/jars/postgresql-42.5.0.jar",
                "/opt/spark/jars/commons-pool2-2.11.1.jar"
            ])
        ) \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://warehouse/logs") \
        .config("spark.sql.catalog.hadoop_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.hadoop_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.hadoop_catalog.s3.access.key.id", "admin") \
        .config("spark.sql.catalog.hadoop_catalog.s3.secret.access.key", "password") \
        .config("spark.sql.catalog.hadoop_catalog.s3.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .getOrCreate()
  finally:
    return SparkSession.builder \
        .appName("Batch Log Processing with Iceberg") \
        .config("spark.ui.port", "4041") \
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar",
                "/opt/spark/jars/hadoop-aws-3.2.2.jar",
                "/opt/spark/jars/aws-java-sdk-1.11.375.jar",
                "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar",
                "/opt/spark/jars/postgresql-42.5.0.jar",
                "/opt/spark/jars/commons-pool2-2.11.1.jar"
            ])
        ) \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://warehouse/logs") \
        .config("spark.sql.catalog.hadoop_catalog.io", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.hadoop_catalog.s3.endpoint", "http://minio:9000") \
        .config("spark.sql.catalog.hadoop_catalog.s3.access.key.id", "admin") \
        .config("spark.sql.catalog.hadoop_catalog.s3.secret.access.key", "password") \
        .config("spark.sql.catalog.hadoop_catalog.s3.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .getOrCreate()


def subscribe_kafka(spark):

  customers_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "customers_dataset") \
      .load()

  geolocation_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "geolocation_dataset") \
      .load()

  order_items_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "order_items_dataset") \
      .load()

  order_payments_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "order_payments_dataset") \
      .load()

  order_reviews_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "order_reviews_dataset") \
      .load()

  orders_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "orders_dataset") \
      .load()

  products_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "products_dataset") \
      .load()

  sellers_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "sellers_dataset") \
      .load()

  product_category_name_translation_df = spark.read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "product_category_name_translation") \
      .load()

  return customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df


def from_value_to_string(customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df):
  customers_df = customers_df.selectExpr("CAST(value AS STRING) as customers_line")
  geolocation_df = geolocation_df.selectExpr("CAST(value AS STRING) as geolocation_line")
  order_items_df = order_items_df.selectExpr("CAST(value AS STRING) as order_items_line")
  order_payments_df = order_payments_df.selectExpr("CAST(value AS STRING) as order_payments_line")
  order_reviews_df = order_reviews_df.selectExpr("CAST(value AS STRING) as order_reviews_line")
  orders_df = orders_df.selectExpr("CAST(value AS STRING) as orders_line")
  products_df = products_df.selectExpr("CAST(value AS STRING) as products_line")
  sellers_df = sellers_df.selectExpr("CAST(value AS STRING) as sellers_line")
  product_category_name_translation_df = product_category_name_translation_df.selectExpr("CAST(value AS STRING) as product_category_name_translation_line")

  return customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df


def cast_string_to_customer_df(customers_df):
  if len(customers_df.head(1)) > 0:
      json_rdd = customers_df.select("customers_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("customer_id", col("message_fields").getItem(0)) \
                         .withColumn("customer_unique_id", col("message_fields").getItem(1)) \
                         .withColumn("customer_zip_code_prefix", col("message_fields").getItem(2)) \
                         .withColumn("customer_city", col("message_fields").getItem(3)) \
                         .withColumn("customer_state", col("message_fields").getItem(4))

      clean_df1 = clean_df[['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state']]

      return clean_df1
  return None

def cast_string_to_geolocation_df(geolocation_df):
  if len(geolocation_df.head(1)) > 0:
      
      json_rdd = geolocation_df.select("geolocation_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("geolocation_zip_code_prefix", col("message_fields").getItem(0)) \
                         .withColumn("geolocation_latitude", col("message_fields").getItem(1)) \
                         .withColumn("geolocation_longitude", col("message_fields").getItem(2)) \
                         .withColumn("geolocation_city", col("message_fields").getItem(3)) \
                         .withColumn("geolocation_state", col("message_fields").getItem(4))
      
      clean_df2 = clean_df[['geolocation_zip_code_prefix','geolocation_latitude','geolocation_longitude','geolocation_city','geolocation_state']]
      return clean_df2
  return None

def cast_string_to_order_items_df(order_items_df):
  if len(order_items_df.head(1)) > 0:
      
      json_rdd = order_items_df.select("order_items_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("order_id", col("message_fields").getItem(0)) \
                         .withColumn("order_item_id", col("message_fields").getItem(1)) \
                         .withColumn("product_id", col("message_fields").getItem(2)) \
                         .withColumn("seller_id", col("message_fields").getItem(3)) \
                         .withColumn("shipping_limit_date", col("message_fields").getItem(4)) \
                         .withColumn("price", col("message_fields").getItem(5)) \
                         .withColumn("freight_value", col("message_fields").getItem(6))
      
      clean_df3 = clean_df[['order_id','order_item_id','product_id','seller_id','shipping_limit_date','price','freight_value']]
      return clean_df3
  return None

def cast_string_to_order_payments_df(order_payments_df):
  if len(order_payments_df.head(1)) > 0:
      
      json_rdd = order_payments_df.select("order_payments_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("order_id", col("message_fields").getItem(0)) \
                         .withColumn("payment_sequential", col("message_fields").getItem(1)) \
                         .withColumn("payment_type", col("message_fields").getItem(2)) \
                         .withColumn("payment_installments", col("message_fields").getItem(3)) \
                         .withColumn("payment_value", col("message_fields").getItem(4))
      
      clean_df4 = clean_df[['order_id','payment_sequential','payment_type','payment_installments','payment_value']]
      return clean_df4
  return None
    

def cast_string_to_order_reviews_df(order_reviews_df):
  if len(order_reviews_df.head(1)) > 0:
      
      json_rdd = order_reviews_df.select("order_reviews_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("review_id", col("message_fields").getItem(0)) \
                         .withColumn("order_id", col("message_fields").getItem(1)) \
                         .withColumn("review_score", col("message_fields").getItem(2)) \
                         .withColumn("review_comment_title", col("message_fields").getItem(3)) \
                         .withColumn("review_comment_message", col("message_fields").getItem(4)) \
                         .withColumn("review_creation_date", col("message_fields").getItem(5)) \
                         .withColumn("review_answer_timestamp", col("message_fields").getItem(6))
      
      clean_df5 = clean_df[['review_id','order_id','review_score','review_comment_title','review_comment_message','review_creation_date','review_answer_timestamp']]
      return clean_df5
  return None

def cast_string_to_orders_df(orders_df):
  if len(orders_df.head(1)) > 0:
      
      json_rdd = orders_df.select("orders_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("order_id", col("message_fields").getItem(0)) \
                         .withColumn("customer_id", col("message_fields").getItem(1)) \
                         .withColumn("order_status", col("message_fields").getItem(2)) \
                         .withColumn("order_purchase_timestamp", col("message_fields").getItem(3)) \
                         .withColumn("order_approved_at", col("message_fields").getItem(4)) \
                         .withColumn("order_delivered_carrier_date", col("message_fields").getItem(5)) \
                         .withColumn("order_delivered_customer_date", col("message_fields").getItem(6)) \
                         .withColumn("order_estimated_delivery_date", col("message_fields").getItem(6))
      
      clean_df6 = clean_df[['order_id','customer_id','order_status','order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']]
      return clean_df6
  return None

def cast_string_to_products_df(products_df):
  if len(products_df.head(1)) > 0:
      
      json_rdd = products_df.select("products_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("product_id", col("message_fields").getItem(0)) \
                         .withColumn("product_category_name", col("message_fields").getItem(1)) \
                         .withColumn("product_name_lenght", col("message_fields").getItem(2)) \
                         .withColumn("product_description_lenght", col("message_fields").getItem(3)) \
                         .withColumn("product_photos_qty", col("message_fields").getItem(4)) \
                         .withColumn("product_weight_g", col("message_fields").getItem(5)) \
                         .withColumn("product_length_cm", col("message_fields").getItem(6)) \
                         .withColumn("product_height_cm", col("message_fields").getItem(7)) \
                         .withColumn("product_width_cm", col("message_fields").getItem(8))
      
      clean_df7 = clean_df[['product_id','product_category_name','product_name_lenght','product_description_lenght','product_photos_qty','product_weight_g','product_length_cm','product_height_cm','product_width_cm']]
      return clean_df7
  return None

def cast_string_to_sellers_df(sellers_df):
  if len(sellers_df.head(1)) > 0:
      
      json_rdd = sellers_df.select("sellers_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("seller_id", col("message_fields").getItem(0)) \
                         .withColumn("seller_zip_code_prefix", col("message_fields").getItem(1)) \
                         .withColumn("seller_city", col("message_fields").getItem(2)) \
                         .withColumn("seller_state", col("message_fields").getItem(3)) 
      
      clean_df8 = clean_df[['seller_id','seller_zip_code_prefix','seller_city','seller_state']]
      return clean_df8
  return None
    

def cast_string_to_product_category_name_translation_df(product_category_name_translation_df):
  if len(product_category_name_translation_df.head(1)) > 0:
      
      json_rdd = product_category_name_translation_df.select("product_category_name_translation_line").rdd.map(lambda r: r[0])
      json_df = spark.read.json(json_rdd)
      
      clean_df = json_df.withColumn("message_clean", regexp_replace(col("message"), '\\"', '"'))
      clean_df = clean_df.withColumn("message_fields", split(col("message_clean"), ","))
      
      clean_df = clean_df.withColumn("product_category_name", col("message_fields").getItem(0)) \
                         .withColumn("product_category_name_english", col("message_fields").getItem(1))
      
      clean_df9 = clean_df[['product_category_name','product_category_name_english']]
      return clean_df9
  return None

def drop_tables(spark):
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.logs")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.customers")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.geolocation")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.order_items")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.order_payments")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.order_reviews")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.orders")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.products")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.sellers")
  spark.sql("DROP TABLE IF EXISTS hadoop_catalog.default.product_category_name_translation")


def create_tables(spark):
  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.customers (
      customer_id STRING,
      customer_unique_id STRING,
      customer_zip_code_prefix STRING,
      customer_city STRING,
      customer_state STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/customers'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.geolocation (
      geolocation_zip_code_prefix STRING,
      geolocation_latitude STRING,
      geolocation_longitude STRING,
      geolocation_city STRING,
      geolocation_state STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/geolocation'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.order_items (
      order_id STRING,
      order_item_id STRING,
      product_id STRING,
      seller_id STRING,
      shipping_limit_date STRING,
      price STRING,
      freight_value STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/order_items'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.order_payments (
      order_id STRING,
      payment_sequential STRING,
      payment_type STRING,
      payment_installments STRING,
      payment_value STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/order_payments'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.order_reviews (
      review_id STRING,
      order_id STRING,
      review_score STRING,
      review_comment_title STRING,
      review_comment_message STRING,
      review_creation_date STRING,
      review_answer_timestamp STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/order_reviews'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.orders (
      order_id STRING,
      customer_id STRING,
      order_status STRING,
      order_purchase_timestamp STRING,
      order_approved_at STRING,
      order_delivered_carrier_date STRING,
      order_delivered_customer_date STRING,
      order_estimated_delivery_date STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/orders'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.products (
      product_id STRING,
      product_category_name STRING,
      product_name_lenght STRING,
      product_description_lenght STRING,
      product_photos_qty STRING,
      product_weight_g STRING,
      product_length_cm STRING,
      product_height_cm STRING,
      product_width_cm STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/products'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.sellers (
      seller_id STRING,
      seller_zip_code_prefix STRING,
      seller_city STRING,
      seller_state STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/sellers'
  """)

  spark.sql("""
  CREATE TABLE IF NOT EXISTS hadoop_catalog.default.product_category_name_translation (
      product_category_name STRING,
      product_category_name_english STRING
  )
  USING iceberg
  LOCATION 's3a://warehouse/logs/default/product_category_name_translation'
  """)


def write_catalog(clean_df1,clean_df2,clean_df3,clean_df4,clean_df5,clean_df6,clean_df7,clean_df8,clean_df9):
  clean_df1.writeTo("hadoop_catalog.default.customers").append()
  clean_df2.writeTo("hadoop_catalog.default.geolocation").append()
  clean_df3.writeTo("hadoop_catalog.default.order_items").append()
  clean_df4.writeTo("hadoop_catalog.default.order_payments").append()
  clean_df5.writeTo("hadoop_catalog.default.order_reviews").append()
  clean_df6.writeTo("hadoop_catalog.default.orders").append()
  clean_df7.writeTo("hadoop_catalog.default.products").append()
  clean_df8.writeTo("hadoop_catalog.default.sellers").append()
  clean_df9.writeTo("hadoop_catalog.default.product_category_name_translation").append()


def write_to_csv(clean_df1,clean_df2,clean_df3,clean_df4,clean_df5,clean_df6,clean_df7,clean_df8,clean_df9):

  output_path = f'/home/iceberg/output/customers'
  clean_df1.coalesce(1) \
      .write \
      .option("header", "true") \
      .mode("overwrite") \
      .csv(output_path)

  output_path = f'/home/iceberg/output/geolocation'
  clean_df2.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/order_items'
  clean_df3.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/order_payments'
  clean_df4.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/order_reviews'
  clean_df5.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/orders'
  clean_df6.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/products'
  clean_df7.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/sellers'
  clean_df8.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

  output_path = f'/home/iceberg/output/product_category_name_translation'
  clean_df9.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)




''' Ejecutar el pipeline '''

'''stop_spark()'''

spark = start_session_spark()

spark.sparkContext.setLogLevel("ERROR")

customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df = subscribe_kafka(spark)

customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df = from_value_to_string(customers_df, geolocation_df, order_items_df, order_payments_df, order_reviews_df, orders_df, products_df, sellers_df, product_category_name_translation_df)

clean_df1 = cast_string_to_customer_df(customers_df)
clean_df2 = cast_string_to_geolocation_df(geolocation_df)
clean_df3 = cast_string_to_order_items_df(order_items_df)
clean_df4 = cast_string_to_order_payments_df(order_payments_df)
clean_df5 = cast_string_to_order_reviews_df(order_reviews_df)
clean_df6 = cast_string_to_orders_df(orders_df)
clean_df7 = cast_string_to_products_df(products_df)
clean_df8 = cast_string_to_sellers_df(sellers_df)
clean_df9 = cast_string_to_product_category_name_translation_df(product_category_name_translation_df)

drop_tables(spark)

create_tables(spark)

write_catalog(clean_df1,clean_df2,clean_df3,clean_df4,clean_df5,clean_df6,clean_df7,clean_df8,clean_df9)

write_to_csv(clean_df1,clean_df2,clean_df3,clean_df4,clean_df5,clean_df6,clean_df7,clean_df8,clean_df9)

df_count = spark.sql("SELECT count(*) FROM hadoop_catalog.default.customers")
df_count.show(truncate=False)

# output_path = "/home/iceberg/output/customers_data"

# clean_df1.coalesce(1).write \
#     .option("header", "true") \
#     .csv(output_path)