# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
import random
from datetime import datetime, timedelta

# Initialize Spark with Delta Lake
spark = SparkSession.builder \
    .appName("ShopEZ-Delta-Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configuration
DB_NAME = "shopez"
TABLE_NAME = "orders"
DELTA_PATH = f"/delta/{DB_NAME}/{TABLE_NAME}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
spark.sql(f"USE {DB_NAME}")

print(f"Environment configured - Database: {DB_NAME}")

# COMMAND ----------

# COMMAND ----------
order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("country", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False)
])

def create_sample_orders(count=50000):
    countries = ["US", "IN", "UK", "DE", "FR", "JP", "CA", "AU", "BR", "SG"]
    currencies = {"US":"USD", "IN":"INR", "UK":"GBP", "DE":"EUR", "FR":"EUR", 
                  "JP":"JPY", "CA":"CAD", "AU":"AUD", "BR":"BRL", "SG":"SGD"}
    statuses = ["CREATED", "PAID", "CANCELLED"]
    
    orders = []
    start_date = datetime(2024, 1, 1)
    
    for i in range(count):
        country = random.choice(countries)
        orders.append((
            f"ORD{i+1:08d}",
            start_date + timedelta(days=random.randint(0, 120), 
                                 hours=random.randint(0, 23), 
                                 minutes=random.randint(0, 59)),
            f"CUST{random.randint(1, 10000):06d}",
            country,
            round(random.uniform(5.0, 1000.0), 2),
            currencies[country],
            random.choice(statuses)
        ))
    return orders

sample_data = create_sample_orders(50000)
orders_df = spark.createDataFrame(sample_data, order_schema)

print(f"Generated {orders_df.count():,} sample orders")
orders_df.show(5)
orders_df.printSchema()

# COMMAND ----------

# COMMAND ----------

orders_with_date = orders_df.withColumn("order_date", to_date(col("order_timestamp")))

print("Added order_date column")
orders_with_date.select("order_timestamp", "order_date").show(5)

# COMMAND ----------

# COMMAND ----------

orders_with_date.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("country", "order_date") \
    .option("path", DELTA_PATH) \
    .saveAsTable(f"{DB_NAME}.{TABLE_NAME}")

print(f"Delta table created: {DB_NAME}.{TABLE_NAME}")
print(f"Storage path: {DELTA_PATH}")

# COMMAND ----------

# COMMAND ----------

table_details = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{TABLE_NAME}")
table_details.select("name", "location", "partitionColumns", "numFiles").show(truncate=False)

partitions = spark.sql(f"SHOW PARTITIONS {DB_NAME}.{TABLE_NAME}")
print(f"Total partitions: {partitions.count()}")
partitions.show(10)

file_stats = spark.sql(f"""
    SELECT country, COUNT(DISTINCT order_date) as date_partitions, 
           COUNT(*) as total_orders
    FROM {DB_NAME}.{TABLE_NAME} 
    GROUP BY country 
    ORDER BY total_orders DESC
""")
print("Partition distribution:")
file_stats.show()

# COMMAND ----------

# COMMAND ----------

print("Query 1: Single country filter (partition pruning)")
us_orders = spark.sql(f"""
    SELECT COUNT(*) as order_count, 
           ROUND(SUM(amount), 2) as total_revenue
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE country = 'US'
""")
us_orders.show()

print("Query 2: Country + date filter (optimal partition pruning)")
specific_query = spark.sql(f"""
    SELECT order_id, customer_id, amount, status
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE country = 'US' AND order_date = '2024-03-15'
    ORDER BY amount DESC
    LIMIT 10
""")
specific_query.show()

print("Execution plan (showing partition pruning):")
specific_query.explain(True)

print("Query 3: Multi-country aggregation")
country_summary = spark.sql(f"""
    SELECT country, 
           COUNT(*) as orders,
           ROUND(AVG(amount), 2) as avg_order_value,
           ROUND(SUM(amount), 2) as total_revenue
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE country IN ('US', 'IN', 'UK')
    GROUP BY country
    ORDER BY total_revenue DESC
""")
country_summary.show()

# COMMAND ----------

# COMMAND ----------

initial_history = spark.sql(f"DESCRIBE HISTORY {DB_NAME}.{TABLE_NAME}")
initial_version = initial_history.collect()[0]["version"]
initial_timestamp = initial_history.collect()[0]["timestamp"]

print(f"Initial version: {initial_version}")
print(f"Initial timestamp: {initial_timestamp}")

initial_status = spark.sql(f"""
    SELECT status, COUNT(*) as count 
    FROM {DB_NAME}.{TABLE_NAME} 
    GROUP BY status 
    ORDER BY count DESC
""")
print("Initial status distribution:")
initial_status.show()

delta_table = DeltaTable.forPath(spark, DELTA_PATH)

delta_table.update(
    condition = "status = 'CREATED' AND amount > 500",
    set = {"status": lit("PAID")}
)

delta_table.update(
    condition = "status = 'CREATED' AND amount < 20",
    set = {"status": lit("CANCELLED")}
)

print("Updates completed")

current_status = spark.sql(f"""
    SELECT status, COUNT(*) as count 
    FROM {DB_NAME}.{TABLE_NAME} 
    GROUP BY status 
    ORDER BY count DESC
""")
print("Current status distribution:")
current_status.show()

print("Time Travel: Querying initial version")
historical_status = spark.sql(f"""
    SELECT status, COUNT(*) as count 
    FROM {DB_NAME}.{TABLE_NAME} VERSION AS OF {initial_version}
    GROUP BY status 
    ORDER BY count DESC
""")
print("Historical status distribution:")
historical_status.show()

print("Time Travel: Querying by timestamp")
timestamp_query = spark.sql(f"""
    SELECT COUNT(*) as total_orders
    FROM {DB_NAME}.{TABLE_NAME} TIMESTAMP AS OF '{initial_timestamp}'
""")
timestamp_query.show()

# COMMAND ----------

# COMMAND ----------

extended_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_timestamp", TimestampType(), False),
    StructField("customer_id", StringType(), False),
    StructField("country", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("status", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("coupon_code", StringType(), True)
])

def create_extended_orders(count=5000):
    countries = ["US", "IN", "UK", "DE", "FR"]
    currencies = {"US":"USD", "IN":"INR", "UK":"GBP", "DE":"EUR", "FR":"EUR"}
    statuses = ["CREATED", "PAID"]
    payment_methods = ["CARD", "UPI", "COD", "WALLET", "BANK_TRANSFER"]
    coupon_codes = ["SAVE10", "WELCOME20", "FIRST50", None, None, None]  # 50% null rate
    
    orders = []
    start_date = datetime(2024, 5, 1)
    
    for i in range(count):
        country = random.choice(countries)
        orders.append((
            f"ORD{i+100001:08d}",
            start_date + timedelta(days=random.randint(0, 30)),
            f"CUST{random.randint(1, 10000):06d}",
            country,
            round(random.uniform(10.0, 800.0), 2),
            currencies[country],
            random.choice(statuses),
            random.choice(payment_methods),
            random.choice(coupon_codes)
        ))
    return orders

extended_data = create_extended_orders(5000)
extended_df = spark.createDataFrame(extended_data, extended_schema)
extended_with_date = extended_df.withColumn("order_date", to_date(col("order_timestamp")))

print("New schema with additional columns:")
extended_with_date.printSchema()

extended_with_date.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("country", "order_date") \
    .option("path", DELTA_PATH) \
    .saveAsTable(f"{DB_NAME}.{TABLE_NAME}")

print("Schema evolution completed")

evolved_table = spark.sql(f"SELECT * FROM {DB_NAME}.{TABLE_NAME} LIMIT 5")
print("Evolved table schema:")
evolved_table.printSchema()

new_columns_sample = spark.sql(f"""
    SELECT payment_method, coupon_code, COUNT(*) as count
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE payment_method IS NOT NULL
    GROUP BY payment_method, coupon_code
    ORDER BY count DESC
    LIMIT 10
""")
print("New columns sample data:")
new_columns_sample.show()

# COMMAND ----------

# COMMAND ----------

print("Update Operation: Marking orders as CANCELLED")

delta_table = DeltaTable.forPath(spark, DELTA_PATH)
delta_table.update(
    condition = "status = 'CREATED' AND amount BETWEEN 100 AND 150",
    set = {"status": lit("CANCELLED")}
)

cancelled_count = spark.sql(f"""
    SELECT COUNT(*) as cancelled_orders 
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE status = 'CANCELLED'
""").collect()[0]["cancelled_orders"]

print(f"Cancelled orders: {cancelled_count:,}")

print("Delete Operation: Removing low-value test orders")

before_count = spark.sql(f"SELECT COUNT(*) as count FROM {DB_NAME}.{TABLE_NAME}").collect()[0]["count"]

delta_table.delete(condition = "amount < 10")

after_count = spark.sql(f"SELECT COUNT(*) as count FROM {DB_NAME}.{TABLE_NAME}").collect()[0]["count"]
deleted_count = before_count - after_count

print(f"Deleted {deleted_count:,} orders (amount < 10)")
print(f"Remaining orders: {after_count:,}")

min_amount = spark.sql(f"SELECT MIN(amount) as min_amount FROM {DB_NAME}.{TABLE_NAME}").collect()[0]["min_amount"]
print(f"New minimum order amount: ${min_amount}")

# COMMAND ----------

# COMMAND ----------

print("Table statistics BEFORE optimization:")
pre_optimize_stats = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{TABLE_NAME}")
pre_optimize_stats.select("numFiles", "sizeInBytes").show()

print("Running OPTIMIZE...")
spark.sql(f"OPTIMIZE {DB_NAME}.{TABLE_NAME}")

print("Running ZORDER on customer_id...")
spark.sql(f"OPTIMIZE {DB_NAME}.{TABLE_NAME} ZORDER BY (customer_id)")

print("Table statistics AFTER optimization:")
post_optimize_stats = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{TABLE_NAME}")
post_optimize_stats.select("numFiles", "sizeInBytes").show()

print("Optimization completed")

# COMMAND ----------

# COMMAND ----------

print("Demonstrating small file problem...")

small_file_data = []
for i in range(100):
    small_file_data.append((
        f"SMALL{i:03d}",
        datetime(2024, 6, 1) + timedelta(hours=i),
        f"CUST{i:06d}",
        f"XX{i%50:02d}",  # 50 different countries
        50.0,
        "USD",
        "CREATED"
    ))

small_df = spark.createDataFrame(small_file_data, order_schema)
small_with_date = small_df.withColumn("order_date", to_date(col("order_timestamp")))

SMALL_TABLE = "orders_small_files"
SMALL_PATH = f"/delta/{DB_NAME}/{SMALL_TABLE}"

small_with_date.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("country", "order_date") \
    .option("path", SMALL_PATH) \
    .saveAsTable(f"{DB_NAME}.{SMALL_TABLE}")

print("Small file problem - BEFORE optimization:")
small_stats_before = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{SMALL_TABLE}")
small_stats_before.select("numFiles", "sizeInBytes").show()

print("Fixing with OPTIMIZE...")
spark.sql(f"OPTIMIZE {DB_NAME}.{SMALL_TABLE}")

print("Small file problem - AFTER optimization:")
small_stats_after = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{SMALL_TABLE}")
small_stats_after.select("numFiles", "sizeInBytes").show()

# COMMAND ----------

# COMMAND ----------

print("SHOPEZ DELTA LAKE PIPELINE - FINAL SUMMARY")
print("=" * 60)

total_orders = spark.sql(f"SELECT COUNT(*) as count FROM {DB_NAME}.{TABLE_NAME}").collect()[0]["count"]
print(f"Total Orders: {total_orders:,}")

status_final = spark.sql(f"""
    SELECT status, COUNT(*) as count, 
           ROUND(SUM(amount), 2) as revenue
    FROM {DB_NAME}.{TABLE_NAME} 
    GROUP BY status 
    ORDER BY count DESC
""")
print("\nFinal Status Distribution:")
status_final.show()

country_final = spark.sql(f"""
    SELECT country, COUNT(*) as orders, 
           ROUND(AVG(amount), 2) as avg_order_value,
           ROUND(SUM(amount), 2) as total_revenue
    FROM {DB_NAME}.{TABLE_NAME} 
    GROUP BY country 
    ORDER BY total_revenue DESC
    LIMIT 5
""")
print("Top Countries by Revenue:")
country_final.show()

payment_dist = spark.sql(f"""
    SELECT payment_method, COUNT(*) as count
    FROM {DB_NAME}.{TABLE_NAME} 
    WHERE payment_method IS NOT NULL
    GROUP BY payment_method 
    ORDER BY count DESC
""")
print("Payment Method Distribution:")
payment_dist.show()

history = spark.sql(f"DESCRIBE HISTORY {DB_NAME}.{TABLE_NAME}")
print(f"\nTable History ({history.count()} versions):")
history.select("version", "timestamp", "operation", "operationParameters").show(5, truncate=False)

final_details = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{TABLE_NAME}")
print("Final Table Details:")
final_details.select("name", "numFiles", "sizeInBytes", "partitionColumns").show(truncate=False)

print("\nALL TASKS COMPLETED SUCCESSFULLY!")
print("Production-ready Delta Lake pipeline implemented")
print("Features: Partitioning, ACID, Time Travel, Schema Evolution, Optimization")