#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyspark>=4.0,<5",
#     "delta-spark>=4.0,<5",
#     "boto3>=1.34",
# ]
# ///
"""
Create a partitioned Delta table on S3 with Spark and register it in the Glue Data Catalog.

Run with uv (dependencies are declared inline, uv resolves them into a throwaway environment):
    uv run scripts/create_delta_table.py --profile personal --region eu-central-1
or, since the file is executable:
    scripts/create_delta_table.py --profile personal --region eu-central-1

Needs a JVM on the PATH: Spark 4 requires Java 17 or 21. The Delta and hadoop-aws jars are resolved through Maven
on the first start (configure_spark_with_delta_pip), so that run needs network access to Maven Central.

What it does:
    1. Writes rows (1,1) (2,2) (3,1) (4,2) as a Delta table partitioned by int_col_partitioned to the S3 location
       (mode overwrite: an existing table at that location is replaced).
    2. Registers the table in Glue as a Delta table: an EXTERNAL_TABLE at the table root
       with table_type=DELTA and spark.sql.sources.provider=delta, the data columns in the StorageDescriptor and
       the partition column as a PartitionKey. Use --no-register to skip this step.
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError

DEFAULT_LOCATION = "s3://simple-s3-glue-database/glue-database-root/default/delta_tables/delta_partitioned_table/"

# The rows of the table: (int_col, int_col_partitioned), mirroring test/sql/hive/test_read_hive.test
ROWS = [(1, 1), (2, 2), (3, 1), (4, 2)]
DATA_COLUMNS = [("int_col", "int")]
PARTITION_COLUMNS = [("int_col_partitioned", "int")]


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--profile",
        default="personal",
        help="AWS profile for S3 and Glue (default: personal)",
    )
    parser.add_argument(
        "--region", default="eu-central-1", help="AWS region (default: eu-central-1)"
    )
    parser.add_argument(
        "--database", default="default", help="Glue database (default: default)"
    )
    parser.add_argument(
        "--table", default="delta_partitioned_table", help="Glue table name"
    )
    parser.add_argument(
        "--location",
        default=DEFAULT_LOCATION,
        help="S3 location of the Delta table root",
    )
    parser.add_argument(
        "--no-register",
        action="store_true",
        help="Only write the Delta table, skip Glue",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace an existing Glue table entry instead of failing on it",
    )
    return parser.parse_args()


def bundled_hadoop_version():
    """The Hadoop version PySpark ships, so hadoop-aws can be pulled in at exactly that version."""
    import glob
    import os
    import re

    import pyspark

    jars = glob.glob(
        os.path.join(
            os.path.dirname(pyspark.__file__), "jars", "hadoop-client-api-*.jar"
        )
    )
    if not jars:
        sys.exit("could not find hadoop-client-api-*.jar in the PySpark installation")
    match = re.search(r"hadoop-client-api-(\d+\.\d+\.\d+)", os.path.basename(jars[0]))
    if not match:
        sys.exit(f"could not parse the Hadoop version from {jars[0]}")
    return match.group(1)


def build_spark(session, region):
    """A local Spark session with Delta enabled and S3 access through the profile's credentials."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    credentials = session.get_credentials().get_frozen_credentials()

    builder = (
        SparkSession.builder.appName("create-delta-table")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", credentials.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", credentials.secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
    )
    if credentials.token:
        builder = builder.config(
            "spark.hadoop.fs.s3a.session.token", credentials.token
        ).config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

    # hadoop-aws must match the Hadoop version bundled with PySpark, mixing versions breaks the S3 client
    hadoop_version = bundled_hadoop_version()
    print(f"using hadoop-aws {hadoop_version} (PySpark's bundled Hadoop version)")
    return configure_spark_with_delta_pip(
        builder, extra_packages=[f"org.apache.hadoop:hadoop-aws:{hadoop_version}"]
    ).getOrCreate()


# Hive type strings (as registered in Glue) to the Spark types written to the Delta log, so that both agree.
# Without an explicit schema Spark infers LongType for Python ints, and Glue would say 'int' while the log
# says 'bigint'.
HIVE_TO_SPARK_TYPE = {
    "int": "IntegerType",
    "bigint": "LongType",
    "string": "StringType",
    "double": "DoubleType",
    "boolean": "BooleanType",
    "date": "DateType",
    "timestamp": "TimestampType",
}


def table_schema():
    from pyspark.sql import types as T

    fields = []
    for name, hive_type in DATA_COLUMNS + PARTITION_COLUMNS:
        if hive_type not in HIVE_TO_SPARK_TYPE:
            sys.exit(f"no Spark type mapping for Hive type '{hive_type}' (column {name})")
        fields.append(
            T.StructField(name, getattr(T, HIVE_TO_SPARK_TYPE[hive_type])(), nullable=True)
        )
    return T.StructType(fields)


def write_delta_table(spark, location):
    # Hadoop speaks s3a://, Glue and DuckDB speak s3://
    s3a_location = "s3a://" + location[len("s3://") :]
    df = spark.createDataFrame(ROWS, schema=table_schema())
    (
        df.write.format("delta")
        .partitionBy(*[name for name, _ in PARTITION_COLUMNS])
        .mode("overwrite")
        .save(s3a_location)
    )
    print(f"wrote {len(ROWS)} rows to {location}")


def register_in_glue(session, database, table, location, replace):
    glue = session.client("glue")
    table_input = {
        "Name": table,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "EXTERNAL": "TRUE",
            "table_type": "DELTA",
            "spark.sql.sources.provider": "delta",
        },
        "StorageDescriptor": {
            "Location": location,
            "Columns": [{"Name": name, "Type": type_} for name, type_ in DATA_COLUMNS],
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": False,
            "NumberOfBuckets": -1,
        },
        "PartitionKeys": [
            {"Name": name, "Type": type_} for name, type_ in PARTITION_COLUMNS
        ],
    }
    try:
        glue.create_table(DatabaseName=database, TableInput=table_input)
        print(f"registered {database}.{table} in Glue")
    except ClientError as error:
        if error.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        if not replace:
            sys.exit(
                f"{database}.{table} already exists in Glue, re-run with --replace to overwrite the entry"
            )
        glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"updated {database}.{table} in Glue")


def main():
    args = parse_args()
    if not args.location.startswith("s3://"):
        sys.exit("--location must be an s3:// URL")
    location = args.location.rstrip("/") + "/"

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    spark = build_spark(session, args.region)
    try:
        write_delta_table(spark, location)
    finally:
        spark.stop()

    if not args.no_register:
        register_in_glue(session, args.database, args.table, location, args.replace)


if __name__ == "__main__":
    main()
