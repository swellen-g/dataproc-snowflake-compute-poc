#!/usr/bin/env python3
"""Snowflake Compute Pipeline Running on Google Cloud Dataproc.

This pipeline offloads heavy data transformation compute from Snowflake
Virtual Warehouses to Google Cloud Dataproc (Apache Spark):

1. Reads source data directly from Snowflake (via the spark-snowflake connector)
   or from an input query/table.
2. Executes distributed transformations in PySpark on Dataproc compute.
3. Writes processed results back to Snowflake (or target data sink: GCS/BigQuery).
4. Outputs exact execution telemetry (runtime, rows processed, partition metrics)
   so the customer can compare actual test performance against Snowflake compute.
"""

from argparse import ArgumentParser
import datetime
import os
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_arguments():
    """Parse pipeline CLI arguments."""
    parser = ArgumentParser(description="Snowflake Compute on Dataproc Pipeline")

    # Snowflake connection parameters
    parser.add_argument("--sf_url", default=os.getenv("SNOWFLAKE_URL"), help="Snowflake account URL (e.g. org-account.snowflakecomputing.com)")
    parser.add_argument("--sf_user", default=os.getenv("SNOWFLAKE_USER"), help="Snowflake username or service user")
    parser.add_argument("--sf_auth_mode", default=os.getenv("SNOWFLAKE_AUTH_MODE", "password"), choices=["password", "keypair", "oauth"], help="Snowflake authentication method for non-human / service accounts")
    parser.add_argument("--sf_password", default=os.getenv("SNOWFLAKE_PASSWORD"), help="Snowflake password (or set via SNOWFLAKE_PASSWORD env var)")
    parser.add_argument("--sf_private_key_path", default=os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), help="Path to a Snowflake private key file used for key-pair auth")
    parser.add_argument("--sf_private_key_passphrase", default=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"), help="Optional passphrase for the private key used with key-pair auth")
    parser.add_argument("--sf_oauth_token", default=os.getenv("SNOWFLAKE_OAUTH_TOKEN"), help="Snowflake OAuth access token for oauth auth mode")
    parser.add_argument("--sf_database", default=os.getenv("SNOWFLAKE_DATABASE"), help="Snowflake database name")
    parser.add_argument("--sf_schema", default=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"), help="Snowflake schema name")
    parser.add_argument("--sf_warehouse", default=os.getenv("SNOWFLAKE_WAREHOUSE"), help="Snowflake warehouse (used briefly for stage read/write)")
    parser.add_argument("--sf_role", default=os.getenv("SNOWFLAKE_ROLE"), help="Snowflake role")

    # Source & Destination options
    parser.add_argument("--sf_source_table", default=None, help="Source Snowflake table name to read from")
    parser.add_argument("--sf_source_query", default=None, help="Source SQL query to execute against Snowflake instead of a full table")
    parser.add_argument("--sf_target_table", default=None, help="Destination Snowflake table to write results to")
    parser.add_argument("--write_mode", default="overwrite", choices=["overwrite", "append", "ignore", "errorifexists"], help="Write mode for output table")

    # Secondary target / staging options
    parser.add_argument("--output_gcs_path", default=None, help="Optional destination GCS path for Parquet output")
    parser.add_argument("--output_bq_table", default=None, help="Optional destination BigQuery table (e.g. project.dataset.table)")
    parser.add_argument("--gcs_temp_bucket", default=os.getenv("GCS_TEMP_BUCKET"), help="Temporary GCS bucket for staging")

    # Offline / Test run flag
    parser.add_argument("--test_mode", action="store_true", help="Run with self-generated sample data to validate pipeline logic without live Snowflake connection")
    parser.add_argument("--test_rows", type=int, default=1_000_000, help="Number of rows to generate in test mode")

    return parser.parse_args()


def init_spark() -> SparkSession:
    """Initialize SparkSession with Cloud Storage and Snowflake connector support."""
    builder = (
        SparkSession.builder
        .appName("Snowflake_Compute_On_Dataproc")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _read_private_key_file(private_key_path: str) -> str:
    """Read a Snowflake private key file from disk for key-pair auth."""
    if not private_key_path:
        raise ValueError("Private key path is required when using Snowflake key-pair auth.")

    key_path = Path(private_key_path)
    if not key_path.exists():
        raise FileNotFoundError(f"Snowflake private key file not found: {private_key_path}")

    return key_path.read_text(encoding="utf-8")


def build_snowflake_options(args) -> dict:
    """Construct Snowflake connector options dictionary.

    Supports both demo/password auth and enterprise service-user authentication via
    key-pair or OAuth tokens.
    """
    if not args.sf_url or not args.sf_user:
        raise ValueError(
            "Missing required Snowflake parameters. Please supply --sf_url and --sf_user, "
            "plus the credentials required for the selected auth mode."
        )

    # Normalize sfURL (strip https:// if provided)
    sf_url = args.sf_url.replace("https://", "").replace("http://", "").strip("/")

    sf_options = {
        "sfURL": sf_url,
        "sfUser": args.sf_user,
        "sfDatabase": args.sf_database,
        "sfSchema": args.sf_schema,
        "sfWarehouse": args.sf_warehouse or "",
    }
    if args.sf_role:
        sf_options["sfRole"] = args.sf_role

    auth_mode = (args.sf_auth_mode or "password").lower()

    if auth_mode == "password":
        sf_options["sfPassword"] = args.sf_password or ""
        return sf_options

    if auth_mode == "keypair":
        sf_options["sfAuthenticator"] = "SNOWFLAKE_JWT"
        sf_options["sfPrivateKey"] = _read_private_key_file(
            args.sf_private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        )
        if args.sf_private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            sf_options["sfPrivateKeyPassphrase"] = (
                args.sf_private_key_passphrase or os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
            )
        return sf_options

    if auth_mode == "oauth":
        token = args.sf_oauth_token or os.getenv("SNOWFLAKE_OAUTH_TOKEN")
        if not token:
            raise ValueError(
                "Snowflake OAuth auth selected but no token was supplied. "
                "Set --sf_oauth_token or SNOWFLAKE_OAUTH_TOKEN."
            )
        sf_options["sfAuthenticator"] = "OAUTH"
        sf_options["sfToken"] = token
        return sf_options

    raise ValueError(f"Unsupported Snowflake auth mode: {auth_mode}")


def read_source_data(spark: SparkSession, args) -> DataFrame:
    """Read source data from Snowflake or generate test data in test mode."""
    if args.test_mode:
        print(f"\n[Mode: TEST] Generating {args.test_rows:,} sample transaction records for pipeline testing...")
        df = spark.range(0, args.test_rows, 1, 50)
        df = (
            df
            .withColumn("transaction_id", F.concat(F.lit("TXN-"), F.col("id")))
            .withColumn("customer_id", F.concat(F.lit("CUST-"), (F.col("id") % 50_000).cast("string")))
            .withColumn("category", F.element_at(
                F.array([F.lit("RETAIL"), F.lit("GROCERY"), F.lit("TRAVEL"), F.lit("FINANCE"), F.lit("HEALTHCARE")]),
                ((F.col("id") % 5) + 1).cast("int")
            ))
            .withColumn("amount", F.round((F.rand(seed=123) * 500) + 10.0, 2))
            .withColumn("tax", F.round(F.col("amount") * 0.08, 2))
            .withColumn("status", F.when(F.col("id") % 40 == 0, "FLAGGED").otherwise("SETTLED"))
            .withColumn(
                "txn_epoch",
                F.lit(int(time.time())) - (F.col("id") % (86400 * 30))
            )
            .withColumn("transaction_time", F.to_timestamp(F.from_unixtime(F.col("txn_epoch"))))
            .withColumn("transaction_date", F.to_date(F.col("transaction_time")))
            .drop("txn_epoch")
        )
        return df

    # Production Snowflake Ingestion
    sf_options = build_snowflake_options(args)
    reader = spark.read.format("net.snowflake.spark.snowflake").options(**sf_options)

    if args.sf_source_query:
        print(f"Reading from Snowflake using custom query:\n{args.sf_source_query}")
        df = reader.option("query", args.sf_source_query).load()
    elif args.sf_source_table:
        print(f"Reading from Snowflake table: {args.sf_database}.{args.sf_schema}.{args.sf_source_table}")
        df = reader.option("dbtable", args.sf_source_table).load()
    else:
        raise ValueError("Either --sf_source_table, --sf_source_query, or --test_mode must be specified.")

    return df


def apply_transformations(df: DataFrame) -> DataFrame:
    """Execute customer business logic & data transformations on Dataproc compute.

    CUSTOMERS: Modify or replace this function with your specific ETL / aggregation logic.
    """
    print("\nExecuting data transformations on Dataproc...")

    # Standardize column naming if reading from Snowflake (uppercase by default)
    columns_lower = {col: col.lower() for col in df.columns}
    for orig_col, lower_col in columns_lower.items():
        if orig_col != lower_col:
            df = df.withColumnRenamed(orig_col, lower_col)

    # 1. Filter out flagged / invalid transactions
    filtered_df = df.filter(F.col("status") != "FLAGGED")

    # 2. Window Analytics: Customer spend ranking and running totals
    customer_window = Window.partitionBy("customer_id").orderBy(F.col("transaction_time").desc())
    running_window = (
        Window.partitionBy("customer_id")
        .orderBy("transaction_time")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    enriched_df = (
        filtered_df
        .withColumn("customer_txn_rank", F.row_number().over(customer_window))
        .withColumn("cumulative_customer_spend", F.round(F.sum("amount").over(running_window), 2))
    )

    # 3. Multi-dimensional Rollup & Aggregation
    aggregated_df = (
        enriched_df
        .groupBy("transaction_date", "category")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("distinct_customers"),
            F.round(F.sum("amount"), 2).alias("total_net_amount"),
            F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
            F.round(F.sum("tax"), 2).alias("total_tax_collected")
        )
        .orderBy("transaction_date", "category")
    )

    return aggregated_df


def write_results(df: DataFrame, args):
    """Write transformed results back to Snowflake, GCS, or BigQuery."""
    # 1. Write back to Snowflake (Primary target)
    if args.sf_target_table and not args.test_mode:
        print(f"\nWriting results back to Snowflake table: {args.sf_target_table} (Mode: {args.write_mode})")
        sf_options = build_snowflake_options(args)
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**sf_options)
            .option("dbtable", args.sf_target_table)
            .mode(args.write_mode)
            .save()
        )
        print("Successfully wrote results to Snowflake.")

    # 2. Optional: Write to Google Cloud Storage (Parquet)
    if args.output_gcs_path:
        print(f"\nWriting results to Cloud Storage: {args.output_gcs_path}")
        df.write.mode(args.write_mode).parquet(args.output_gcs_path)
        print("Successfully wrote results to GCS.")

    # 3. Optional: Write to BigQuery
    if args.output_bq_table:
        print(f"\nWriting results to BigQuery: {args.output_bq_table}")
        writer = (
            df.write
            .format("bigquery")
            .option("table", args.output_bq_table)
            .mode(args.write_mode)
        )
        if args.gcs_temp_bucket:
            writer = writer.option("temporaryGcsBucket", args.gcs_temp_bucket)
        writer.save()
        print("Successfully wrote results to BigQuery.")


def main():
    args = parse_arguments()
    start_time = time.time()

    print("=" * 80)
    print("Dataproc PySpark Pipeline for Snowflake Compute Offload")
    print(f"Timestamp: {datetime.datetime.now(datetime.timezone.utc).isoformat()}")
    print("=" * 80)

    spark = init_spark()

    try:
        # Step 1: Ingest
        source_df = read_source_data(spark, args)
        input_count = source_df.count()
        print(f"Loaded {input_count:,} records from source.")
        print("\nSource Schema:")
        source_df.printSchema()

        # Step 2: Transform
        transform_start = time.time()
        transformed_df = apply_transformations(source_df)
        transformed_count = transformed_df.count()
        transform_duration = time.time() - transform_start

        print(f"\nTransformation complete: {transformed_count:,} summary rows generated in {transform_duration:.2f}s.")
        print("\nTransformed Output Preview:")
        transformed_df.show(5, truncate=False)

        # Step 3: Write Output
        write_results(transformed_df, args)

        total_elapsed = time.time() - start_time

        # Step 4: Output Test Metrics for Customer Comparison
        print("\n" + "=" * 80)
        print("                       DATAPROC EXECUTION METRICS")
        print("=" * 80)
        print(f"Input Rows Processed       : {input_count:,}")
        print(f"Output Rows Generated      : {transformed_count:,}")
        print(f"Total Pipeline Runtime     : {total_elapsed:.2f} seconds ({total_elapsed / 60.0:.2f} minutes)")
        print(f"Transformation Wall Time   : {transform_duration:.2f} seconds")
        print("-" * 80)
        print("Next Step for Testing:")
        print("1. Record your 'Total Pipeline Runtime' above.")
        print("2. Check Snowflake Query History / Warehouse Metering for the equivalent query.")
        print("3. Compare execution duration and compute cost using your actual contract rates.")
        print("=" * 80 + "\n")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

