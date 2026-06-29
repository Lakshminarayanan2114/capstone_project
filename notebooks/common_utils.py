# Databricks notebook source


# COMMAND ----------

import json
import glob
import os
from datetime import datetime
import uuid

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

METADATA_PATH = "/Volumes/capstone_project/raw_source/raw_datasets/raw/meta_data"

# COMMAND ----------

import json

def get_framework_config():

    framework_path = (
        METADATA_PATH + "/meta_framework.json"
    )

    with open(framework_path, "r") as f:

        framework = json.load(f)

    return framework["framework_config"]

# COMMAND ----------

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def load_framework_metadata():
    return load_json(f"{METADATA_PATH}/meta_framework.json")


def load_gold_metadata():
    return load_json(f"{METADATA_PATH}/meta_gold.json")


# COMMAND ----------

def _build_source_registry():
    """
    Returns dict  { "Orders": {...config...}, "Products": {...}, ... }
    for every active source found in the metadata folder.
    """
    pattern = f"{METADATA_PATH}/meta_sources_*.json"
    registry = {}
    for filepath in glob.glob(pattern):
        data = load_json(filepath)
        # Each file has exactly one top-level key = source name
        for source_name, config in data.items():
            if config.get("active", True):          # default active=True if key missing
                registry[source_name] = config
            else:
                print(f"[common_utils] Skipping inactive source: {source_name}")
    return registry

# COMMAND ----------

_SOURCE_REGISTRY = _build_source_registry()
print(f"[common_utils] Active sources: {list(_SOURCE_REGISTRY.keys())}")

# COMMAND ----------

def get_active_source_names():
    """Return list of all active source names."""
    return list(_SOURCE_REGISTRY.keys())

# COMMAND ----------

def get_source_config(source_name):
    if source_name not in _SOURCE_REGISTRY:
        raise KeyError(
            f"Source '{source_name}' not found or not active. "
            f"Active sources: {get_active_source_names()}"
        )
    return _SOURCE_REGISTRY[source_name]


def get_column_metadata(source_name):
    return get_source_config(source_name)["columns"]


def get_validation_rules(source_name):
    return get_source_config(source_name).get("validation_rules", {})


def get_incremental_config(source_name):
    return get_source_config(source_name).get("incremental_config", {})


def get_scd_config(source_name):
    return get_source_config(source_name).get("scd_config", {})


def get_execution_audit_config():
    return load_framework_metadata()["framework_config"]["execution_audit_policy"]

# COMMAND ----------

def read_source(source_name):
    """
    Reads a source file from the Volume using config from metadata.
    Dispatches on  format  (CSV / JSON / PARQUET / DELTA).
    source_location must be an absolute path in the metadata JSON.
    """
    config = get_source_config(source_name)
    fmt    = config["format"].upper()
    path   = config["source_location"]

    print(f"[read_source] {source_name} | format={fmt} | path={path}")

    if fmt == "CSV":
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(path)
        )

    elif fmt == "JSON":
        return spark.read.option("multiline", True).option("inferSchema", False).json(path)

    elif fmt == "PARQUET":
        return spark.read.parquet(path)

    elif fmt == "DELTA":
        return spark.read.format("delta").load(path)

    else:
        raise ValueError(
            f"[read_source] Unsupported format '{fmt}' for source '{source_name}'. "
            f"Supported: CSV, JSON, PARQUET, DELTA."
        )


# COMMAND ----------

_AUDIT_SCHEMA = StructType([
    StructField("RunID",         StringType(),    True),
    StructField("PipelineRunID", StringType(),    True),
    StructField("Layer",         StringType(),    True),
    StructField("SourceName",    StringType(),    True),
    StructField("TargetTable",   StringType(),    True),
    StructField("LoadStrategy",  StringType(),    True),
    StructField("Status",        StringType(),    True),
    StructField("RowsRead",      LongType(),      True),
    StructField("RowsWritten",   LongType(),      True),
    StructField("RowsRejected",  LongType(),      True),
    StructField("StartTime",     TimestampType(), True),
    StructField("EndTime",       TimestampType(), True),
    StructField("DurationSecs",  DoubleType(),    True),
    StructField("ErrorMessage",  StringType(),    True),
    StructField("CreatedOn",     TimestampType(), True),
])


def create_audit_table_if_not_exists():
    config         = get_execution_audit_config()
    full_table     = f"{config['catalog']}.{config['schema']}.{config['table']}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            RunID         STRING,
            PipelineRunID STRING,
            Layer         STRING,
            SourceName    STRING,
            TargetTable   STRING,
            LoadStrategy  STRING,
            Status        STRING,
            RowsRead      BIGINT,
            RowsWritten   BIGINT,
            RowsRejected  BIGINT,
            StartTime     TIMESTAMP,
            EndTime       TIMESTAMP,
            DurationSecs  DOUBLE,
            ErrorMessage  STRING,
            CreatedOn     TIMESTAMP
        ) USING DELTA
    """)
    print("Audit table ready.")


# COMMAND ----------

def create_audit_table_if_not_exists():
    config         = get_execution_audit_config()
    full_table     = f"{config['catalog']}.{config['schema']}.{config['table']}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['catalog']}.{config['schema']}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            RunID         STRING,
            PipelineRunID STRING,
            Layer         STRING,
            SourceName    STRING,
            TargetTable   STRING,
            LoadStrategy  STRING,
            Status        STRING,
            RowsRead      BIGINT,
            RowsWritten   BIGINT,
            RowsRejected  BIGINT,
            StartTime     TIMESTAMP,
            EndTime       TIMESTAMP,
            DurationSecs  DOUBLE,
            ErrorMessage  STRING,
            CreatedOn     TIMESTAMP
        ) USING DELTA
    """)
    print("Audit table ready.")


def get_pipeline_run_id():
    try:
        return dbutils.widgets.get("pipelineRunId")
    except Exception:
        return "MANUAL_RUN"


def start_audit(
    layer,
    object_name,
    object_type="source",
    pipeline_run_id=None
):

    if pipeline_run_id is None:
        pipeline_run_id = get_pipeline_run_id()

    if object_type == "source":

        config = get_source_config(object_name)

        if layer == "Bronze":
            target_table = config["bronze_table"]
            load_strategy = "Append"

        elif layer == "Silver":
            target_table = config["silver_table"]

            if get_scd_config(object_name):
                load_strategy = "SCD2"

            elif get_incremental_config(object_name):
                load_strategy = "Incremental"

            else:
                load_strategy = "Full Refresh"

        else:
            target_table = object_name
            load_strategy = layer

    elif object_type == "dimension":

        target_table = object_name
        load_strategy = "Dimension Load"

    elif object_type == "fact":

        target_table = object_name
        load_strategy = "Fact Load"

    else:

        target_table = object_name
        load_strategy = "Unknown"

    return {

        "RunID": str(uuid.uuid4()),

        "PipelineRunID": str(pipeline_run_id),

        "Layer": layer,

        "SourceName": object_name,

        "TargetTable": target_table,

        "LoadStrategy": load_strategy

    }


def complete_audit(
    audit,
    rows_read,
    rows_written,
    rows_rejected=0,
    status="SUCCESS",
    error_message=None
):

    audit["Status"] = status
    audit["RowsRead"] = int(rows_read)
    audit["RowsWritten"] = int(rows_written)
    audit["RowsRejected"] = int(rows_rejected)
    audit["DurationSecs"] = 0.0
    audit["ErrorMessage"] = error_message or ""

    return audit


def write_audit(audit):

    config = get_execution_audit_config()

    full_table = f"{config['catalog']}.{config['schema']}.{config['table']}"

    audit_df = (
        spark.range(1)
        .select(

            lit(audit["RunID"]).alias("RunID"),

            lit(audit["PipelineRunID"]).alias("PipelineRunID"),

            lit(audit["Layer"]).alias("Layer"),

            lit(audit["SourceName"]).alias("SourceName"),

            lit(audit["TargetTable"]).alias("TargetTable"),

            lit(audit["LoadStrategy"]).alias("LoadStrategy"),

            lit(audit["Status"]).alias("Status"),

            lit(audit["RowsRead"]).cast("bigint").alias("RowsRead"),

            lit(audit["RowsWritten"]).cast("bigint").alias("RowsWritten"),

            lit(audit["RowsRejected"]).cast("bigint").alias("RowsRejected"),

            current_timestamp().alias("StartTime"),

            current_timestamp().alias("EndTime"),

            lit(audit["DurationSecs"]).cast("double").alias("DurationSecs"),

            lit(audit["ErrorMessage"]).alias("ErrorMessage"),

            current_timestamp().alias("CreatedOn")
        )
    )

    (
        audit_df.write
        .format("delta")
        .mode(config["write_mode"])
        .saveAsTable(full_table)
    )

    print("Audit log written successfully.")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
def add_bronze_audit_columns(df):
    return (
        df
        .withColumn("_IngestionTimestamp",  current_timestamp())
    )


def add_silver_audit_columns(df):
    return (
        df
        .withColumn("_ProcessedTimestamp", current_timestamp())
        .withColumn("_IsRejected",         lit(False))
    )


# ------------------------------------------------------------------
# 7.  WRITE HELPERS
# ------------------------------------------------------------------

def write_layer(df, table_name, mode="append"):
    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(table_name)
    )


def apply_incremental_filter(df, source_name, last_watermark):
    config = get_incremental_config(source_name)
    if not config:
        return df
    watermark_column = config["watermark_column"]
    if last_watermark is None:
        return df
    return df.filter(col(watermark_column) > last_watermark)


# ------------------------------------------------------------------
# 8.  GOLD METADATA HELPERS
# ------------------------------------------------------------------

def get_dimension_config(dimension_name):
    return load_gold_metadata()["dimensions"].get(dimension_name)


def get_fact_config(fact_name):
    return load_gold_metadata()["facts"].get(fact_name)


def get_all_dimensions():
    return load_gold_metadata()["dimensions"]


def get_all_facts():
    return load_gold_metadata()["facts"]

# COMMAND ----------

framework_config = load_framework_metadata()
print(f"[common_utils] Framework keys: {list(framework_config.keys())}")

# COMMAND ----------

