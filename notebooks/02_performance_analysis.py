# Databricks notebook source

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder.getOrCreate()
DB_NAME = "shopez"
TABLE_NAME = "orders"

# COMMAND ----------

# COMMAND ----------

def measure_query_time(query_func):
    start_time = time.time()
    result = query_func()
    end_time = time.time()
    return result, end_time - start_time

print("Performance Test 1: Full Table Scan vs Partition Pruning")

def full_scan():
    return spark.sql(f"SELECT COUNT(*) FROM {DB_NAME}.{TABLE_NAME}").collect()

def partition_pruned():
    return spark.sql(f"""
        SELECT COUNT(*) FROM {DB_NAME}.{TABLE_NAME} 
        WHERE country = 'US' AND order_date = '2024-03-15'
    """).collect()

_, full_scan_time = measure_query_time(full_scan)
_, pruned_time = measure_query_time(partition_pruned)

print(f"Full scan time: {full_scan_time:.3f} seconds")
print(f"Partition pruned time: {pruned_time:.3f} seconds")
print(f"Performance improvement: {(full_scan_time/pruned_time):.1f}x faster")

# COMMAND ----------

# COMMAND ----------

file_analysis = spark.sql(f"""
    SELECT 
        country,
        COUNT(DISTINCT order_date) as date_partitions,
        COUNT(*) as total_orders,
        ROUND(AVG(amount), 2) as avg_order_value
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY country
    ORDER BY total_orders DESC
""")

print("Partition Distribution Analysis:")
file_analysis.show()

table_details = spark.sql(f"DESCRIBE DETAIL {DB_NAME}.{TABLE_NAME}")
print("Table Storage Details:")
table_details.select("numFiles", "sizeInBytes", "partitionColumns").show()

# COMMAND ----------

# COMMAND ----------

print("Common E-commerce Query Patterns:")

daily_revenue = spark.sql(f"""
    SELECT 
        order_date,
        country,
        COUNT(*) as orders,
        ROUND(SUM(amount), 2) as revenue
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE order_date >= '2024-03-01' AND order_date <= '2024-03-31'
    GROUP BY order_date, country
    ORDER BY order_date, revenue DESC
""")
print("Daily Revenue by Country (March 2024):")
daily_revenue.show(20)

customer_analysis = spark.sql(f"""
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        ROUND(SUM(amount), 2) as total_spent,
        ROUND(AVG(amount), 2) as avg_order_value,
        MAX(order_date) as last_order_date
    FROM {DB_NAME}.{TABLE_NAME}
    GROUP BY customer_id
    HAVING order_count > 5
    ORDER BY total_spent DESC
    LIMIT 10
""")
print("Top Customers (5+ orders):")
customer_analysis.show()

payment_analysis = spark.sql(f"""
    SELECT 
        payment_method,
        COUNT(*) as transactions,
        ROUND(AVG(amount), 2) as avg_transaction_value,
        ROUND(SUM(amount), 2) as total_volume
    FROM {DB_NAME}.{TABLE_NAME}
    WHERE payment_method IS NOT NULL
    GROUP BY payment_method
    ORDER BY total_volume DESC
""")
print("Payment Method Analysis:")
payment_analysis.show()

# COMMAND ----------

# COMMAND ----------

optimization_history = spark.sql(f"""
    SELECT version, timestamp, operation, operationParameters
    FROM (DESCRIBE HISTORY {DB_NAME}.{TABLE_NAME})
    WHERE operation IN ('OPTIMIZE', 'WRITE')
    ORDER BY version DESC
""")
print("Optimization History:")
optimization_history.show(truncate=False)

print("Performance Summary:")
print("Partitioning Strategy: country + order_date")
print("Z-Order Optimization: customer_id")
print("File Compaction: OPTIMIZE command")
print("Schema Evolution: Seamless column addition")
print("ACID Compliance: Full transaction support")

# COMMAND ----------

# COMMAND ----------

print("PERFORMANCE RECOMMENDATIONS:")
print("=" * 50)
print("1. Query Patterns:")
print("   - Always include country in WHERE clause")
print("   - Use date ranges for time-based queries")
print("   - Leverage customer_id for user analytics")
print()
print("2. Maintenance:")
print("   - Run OPTIMIZE weekly")
print("   - VACUUM old files monthly")
print("   - Monitor partition count vs file size")
print()
print("3. Scaling:")
print("   - Consider date-based partitioning for high volume")
print("   - Implement auto-optimization for production")
print("   - Use Delta Lake liquid clustering for complex queries")
print()
print("4. Data Quality:")
print("   - Implement constraint validation")
print("   - Set up data quality monitoring")
print("   - Use Delta Lake expectations for SLA compliance")