"""
PySpark Job: bronze_to_silver.py
Author: Ashok Chowdary (Ashok98765vvs)
Description:
    Transforms raw Bronze Layer data (Azure ADLS Gen2) into cleaned,
    conformed Silver Layer using PySpark + Delta Lake.
    Applies deduplication, schema enforcement, null handling,
    and type casting. Part of the Medallion Architecture pipeline.
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType, BooleanType
)
from delta.tables import DeltaTable

# ─────────────────────────────────────────
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ─────────────────────────────────────────
# SCHEMA DEFINITIONS
# ─────────────────────────────────────────
SALES_BRONZE_SCHEMA = StructType([
    StructField('sale_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('product_id', StringType(), True),
    StructField('sale_date', StringType(), True),
    StructField('quantity', StringType(), True),
    StructField('unit_price', StringType(), True),
    StructField('total_amount', StringType(), True),
    StructField('currency', StringType(), True),
    StructField('region', StringType(), True),
    StructField('is_returned', StringType(), True),
    StructField('ingested_at', StringType(), True),
])

SALES_SILVER_SCHEMA = StructType([
    StructField('sale_id', StringType(), False),
    StructField('customer_id', StringType(), False),
    StructField('product_id', StringType(), False),
    StructField('sale_date', TimestampType(), False),
    StructField('quantity', LongType(), False),
    StructField('unit_price', DoubleType(), False),
    StructField('total_amount', DoubleType(), False),
    StructField('currency', StringType(), False),
    StructField('region', StringType(), True),
    StructField('is_returned', BooleanType(), False),
    StructField('ingested_at', TimestampType(), True),
    StructField('processed_at', TimestampType(), False),
    StructField('partition_date', StringType(), False),
])


def create_spark_session() -> SparkSession:
    """Initialize SparkSession with Delta Lake support."""
    spark = (
        SparkSession.builder
        .appName('BronzeToSilver_SalesPipeline')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.databricks.delta.optimizeWrite.enabled', 'true')
        .config('spark.databricks.delta.autoCompact.enabled', 'true')
        .config('spark.sql.shuffle.partitions', '200')
        .config('spark.default.parallelism', '200')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    return spark


def read_bronze_data(spark: SparkSession, bronze_path: str, execution_date: str):
    """Read raw Bronze Layer data from ADLS Gen2."""
    full_path = f"{bronze_path}/{execution_date}/"
    logger.info(f"Reading Bronze data from: {full_path}")

    df = spark.read.schema(SALES_BRONZE_SCHEMA).json(full_path)
    record_count = df.count()
    logger.info(f"Bronze records loaded: {record_count:,}")

    if record_count == 0:
        raise ValueError(f"No data found in Bronze path: {full_path}")

    return df


def clean_and_transform(df, execution_date: str):
    """Apply all Silver Layer transformations."""
    logger.info("Applying Bronze to Silver transformations...")

    # Step 1: Drop exact duplicates
    initial_count = df.count()
    df = df.dropDuplicates(['sale_id'])
    dedup_count = df.count()
    logger.info(f"Deduplication: {initial_count:,} -> {dedup_count:,} (removed {initial_count - dedup_count:,})")

    # Step 2: Drop rows with critical nulls
    critical_cols = ['sale_id', 'customer_id', 'product_id', 'sale_date', 'total_amount']
    df = df.dropna(subset=critical_cols)
    after_null_drop = df.count()
    logger.info(f"Null removal: {dedup_count:,} -> {after_null_drop:,} rows remaining")

    # Step 3: Type casting with error handling
    df = (
        df
        .withColumn('sale_date', F.to_timestamp(F.col('sale_date'), 'yyyy-MM-dd'))
        .withColumn('quantity', F.col('quantity').cast(LongType()))
        .withColumn('unit_price', F.col('unit_price').cast(DoubleType()))
        .withColumn('total_amount', F.col('total_amount').cast(DoubleType()))
        .withColumn('is_returned',
            F.when(F.lower(F.col('is_returned')).isin('true', '1', 'yes'), True)
            .otherwise(False)
        )
        .withColumn('ingested_at', F.to_timestamp(F.col('ingested_at')))
    )

    # Step 4: Data quality filters (business rules)
    df = df.filter(
        (F.col('total_amount') > 0) &
        (F.col('quantity') > 0) &
        (F.col('unit_price') > 0) &
        (F.col('sale_date') >= F.lit('2020-01-01').cast(TimestampType()))
    )

    # Step 5: Standardize string columns
    df = (
        df
        .withColumn('currency', F.upper(F.trim(F.col('currency'))))
        .withColumn('region', F.initcap(F.trim(F.col('region'))))
        .withColumn('customer_id', F.trim(F.col('customer_id')))
        .withColumn('product_id', F.trim(F.col('product_id')))
    )

    # Step 6: Add pipeline metadata columns
    df = (
        df
        .withColumn('processed_at', F.current_timestamp())
        .withColumn('partition_date', F.lit(execution_date))
    )

    # Step 7: Calculate derived metrics
    df = df.withColumn(
        'profit_margin',
        F.round((F.col('total_amount') - (F.col('quantity') * F.col('unit_price'))) / F.col('total_amount') * 100, 2)
    )

    final_count = df.count()
    logger.info(f"Final Silver record count: {final_count:,}")
    logger.info(f"Data quality pass rate: {final_count/initial_count*100:.2f}%")

    return df


def write_to_silver(df, silver_path: str, execution_date: str) -> None:
    """Write transformed data to Silver Layer using Delta Lake MERGE (upsert)."""
    logger.info(f"Writing to Silver Layer: {silver_path}")

    if DeltaTable.isDeltaTable(df.sparkSession, silver_path):
        # MERGE (upsert) for incremental loads
        delta_table = DeltaTable.forPath(df.sparkSession, silver_path)
        (
            delta_table.alias('target')
            .merge(
                df.alias('source'),
                'target.sale_id = source.sale_id'
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Delta MERGE (upsert) completed successfully.")
    else:
        # Initial full load
        (
            df.write
            .format('delta')
            .mode('overwrite')
            .partitionBy('partition_date')
            .option('overwriteSchema', 'true')
            .save(silver_path)
        )
        logger.info("Initial Delta write completed successfully.")

    # Optimize and Z-order for query performance
    df.sparkSession.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (customer_id, sale_date)")
    logger.info("Delta OPTIMIZE with Z-ORDER complete.")


def generate_data_profile(df) -> dict:
    """Generate a data quality summary profile."""
    profile = {
        'total_records': df.count(),
        'unique_customers': df.select('customer_id').distinct().count(),
        'unique_products': df.select('product_id').distinct().count(),
        'date_range_min': df.agg(F.min('sale_date')).collect()[0][0],
        'date_range_max': df.agg(F.max('sale_date')).collect()[0][0],
        'total_revenue': df.agg(F.sum('total_amount')).collect()[0][0],
        'avg_order_value': df.agg(F.avg('total_amount')).collect()[0][0],
        'return_rate_pct': df.filter(F.col('is_returned') == True).count() / df.count() * 100,
        'null_customer_pct': df.filter(F.col('customer_id').isNull()).count() / df.count() * 100,
    }
    logger.info("Data Profile Summary:")
    for k, v in profile.items():
        logger.info(f"  {k}: {v}")
    return profile


def main(execution_date: str = None):
    """Main entry point for the Bronze to Silver Spark job."""
    execution_date = execution_date or datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Starting Bronze to Silver transformation for: {execution_date}")

    # Azure ADLS Gen2 paths
    STORAGE_ACCOUNT = 'yourstorageaccount'
    CONTAINER = 'lakehouse'
    BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/sales"
    SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/sales"

    spark = create_spark_session()

    try:
        # Read
        bronze_df = read_bronze_data(spark, BRONZE_PATH, execution_date)

        # Transform
        silver_df = clean_and_transform(bronze_df, execution_date)

        # Profile
        profile = generate_data_profile(silver_df)

        # Write
        write_to_silver(silver_df, SILVER_PATH, execution_date)

        logger.info(f"Bronze to Silver job completed successfully. Records: {profile['total_records']:,}")
        return profile

    except Exception as e:
        logger.error(f"Bronze to Silver job FAILED: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == '__main__':
    execution_date = sys.argv[1] if len(sys.argv) > 1 else None
    main(execution_date)
