# Databricks notebook source
# MAGIC %run /Workspace/Users/kuttankutt2111@gmail.com/project_capstone/common_utils

# COMMAND ----------

def read_bronze(source_name):

    source_config = get_source_config(source_name)

    bronze_table = source_config["bronze_table"]

    full_table = f"capstone_project.bronze_layer.{bronze_table}"

    print(f"[read_bronze] Reading {full_table}")

    return spark.table(full_table)

# COMMAND ----------

from pyspark.sql.types import *

TYPE_MAPPING = {

    "int": IntegerType(),

    "double": DoubleType(),

    "string": StringType(),

    "date": DateType(),

    "timestamp": TimestampType()
}

# COMMAND ----------

from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col, explode_outer

def flatten_df(df):

    complex_fields = {

        field.name: field.dataType

        for field in df.schema.fields

        if isinstance(
            field.dataType,
            (StructType, ArrayType)
        )
    }

    while len(complex_fields) != 0:

        column_name = list(
            complex_fields.keys()
        )[0]

        datatype = complex_fields[
            column_name
        ]

        if isinstance(
            datatype,
            StructType
        ):

            expanded_cols = [

                col(
                    f"{column_name}.{nested_field.name}"
                ).alias(
                    f"{column_name}_{nested_field.name}"
                )

                for nested_field in datatype.fields

            ]

            df = (

                df

                .select(
                    "*",
                    *expanded_cols
                )

                .drop(
                    column_name
                )

            )

        elif isinstance(
            datatype,
            ArrayType
        ):

            df = (

                df

                .withColumn(
                    column_name,
                    explode_outer(
                        col(column_name)
                    )
                )

            )

        complex_fields = {

            field.name: field.dataType

            for field in df.schema.fields

            if isinstance(
                field.dataType,
                (StructType, ArrayType)
            )
        }

    return df

# COMMAND ----------

def prepare_source(
    df,
    source_name
):

    source_config = get_source_config(
        source_name
    )

    file_format = (
        source_config["format"]
        .lower()
    )

    if file_format == "json":

        print(
            f"{source_name} : JSON Detected - Flattening"
        )

        df = flatten_df(
            df
        )

    return df

# COMMAND ----------

from pyspark.sql.functions import col, to_date, to_timestamp

def apply_schema(
    df,
    source_name
):

    column_metadata = get_column_metadata(
        source_name
    )

    for column_name, metadata in column_metadata.items():

        target_type = metadata[
            "datatype"
        ]

        column_format = metadata.get(
            "format"
        )


        if target_type == "date":

            if column_format:

                df = df.withColumn(
                    column_name,
                    to_date(
                        col(column_name),
                        column_format
                    )
                )

            else:

                df = df.withColumn(
                    column_name,
                    col(column_name).cast("date")
                )


        elif target_type == "timestamp":

            if column_format:

                df = df.withColumn(
                    column_name,
                    to_timestamp(
                        col(column_name),
                        column_format
                    )
                )

            else:

                df = df.withColumn(
                    column_name,
                    col(column_name).cast("timestamp")
                )

        else:

            df = df.withColumn(
                column_name,
                col(column_name).cast(
                    TYPE_MAPPING[target_type]
                )
            )

    return df

# COMMAND ----------

from pyspark.sql.functions import col, max

def apply_incremental_filter(df, source_name):

    config = get_incremental_config(source_name)

    # No incremental config -> process normally
    if (
        not config
        or "watermark_column" not in config
    ):
        return df

    watermark_column = config["watermark_column"]

    source_config = get_source_config(source_name)

    silver_table = source_config["silver_table"]

    table_name = f"capstone_project.silver_layer.{silver_table}"

    # Initial load
    if not spark.catalog.tableExists(table_name):

        print(f"{source_name}: Initial Silver Load")

        return df

    last_watermark = (
        spark.table(table_name)
        .agg(max(col(watermark_column)))
        .collect()[0][0]
    )

    print(f"{source_name}: Silver Watermark = {last_watermark}")

    if last_watermark is None:
        return df

    return df.filter(
        col(watermark_column) > last_watermark
    )

# COMMAND ----------

def initialize_rejection_columns(df):

    return (

        df

        .withColumn(
            "RejectReason",
            lit("")
        )

        .withColumn(
            "_IsRejected",
            lit(False)
        )

    )

# COMMAND ----------

def validate_mandatory_columns(
    df,
    source_name
):

    column_metadata = get_column_metadata(
        source_name
    )

    for column_name, metadata in column_metadata.items():

        if metadata["nullable"] == False:

            df = df.withColumn(

                "RejectReason",

                when(
                    col(column_name).isNull(),

                    concat(
                        col("RejectReason"),
                        lit(f"{column_name} is NULL; ")
                    )
                ).otherwise(
                    col("RejectReason")
                )
            )

    return df

# COMMAND ----------

def validate_business_rules(
    df,
    source_name
):

    validation_rules = get_validation_rules(
        source_name
    )

    for column_name, rule in validation_rules.items():

        rule_type = rule["rule_type"]

        if rule_type == "greater_than":

            threshold = rule["value"]

            df = df.withColumn(

                "RejectReason",

                when(

                    col(column_name) <= threshold,

                    concat(
                        col("RejectReason"),
                        lit(
                            f"{column_name} <= {threshold}; "
                        )
                    )

                ).otherwise(
                    col("RejectReason")
                )
            )

    return df

# COMMAND ----------

def validate_primary_key(
    df,
    source_name
):

    column_metadata = get_column_metadata(
        source_name
    )

    pk_columns = []

    for column_name, metadata in column_metadata.items():

        if metadata.get("key_type") == "PK":

            pk_columns.append(
                column_name
            )

    if len(pk_columns) == 0:

        return df

    duplicate_df = (

        df.groupBy(
            pk_columns
        )
        .count()
        .filter(
            col("count") > 1
        )
    )

    df = (
        df.alias("src")
        .join(
            duplicate_df.alias("dup"),
            pk_columns,
            "left"
        )
    )

    df = df.withColumn(

        "RejectReason",

        when(

            col("count").isNotNull(),

            concat(
                col("RejectReason"),
                lit("Duplicate Primary Key; ")
            )

        ).otherwise(
            col("RejectReason")
        )
    )

    return df.drop("count")

# COMMAND ----------

from pyspark.sql.functions import *

def validate_foreign_keys(
    df,
    source_name
):

    column_metadata = get_column_metadata(
        source_name
    )

    for column_name, metadata in column_metadata.items():

        if metadata.get("key_type") != "FK":
            continue

        reference_table = metadata[
            "reference_table"
        ]

        reference_column = metadata[
            "reference_column"
        ]

        full_table_name = (
            f"capstone_project.silver_layer.{reference_table}"
        )

        if not spark.catalog.tableExists(
            full_table_name
        ):

            print(
                f"Skipping FK Validation : {reference_table} not found"
            )

            continue

        reference_df = spark.table(
            full_table_name
        )

        # Handle SCD2 dimensions
        if "IsCurrent" in reference_df.columns:

            reference_df = (
                reference_df
                .filter(
                    col("IsCurrent") == True
                )
            )

        reference_df = (

            reference_df

            .select(
                col(reference_column)
            )

            .distinct()

            .withColumnRenamed(
                reference_column,
                f"ref_{reference_column}"
            )

        )

        df = (

            df.alias("src")

            .join(

                reference_df.alias("ref"),

                col(
                    f"src.{column_name}"
                )

                ==

                col(
                    f"ref.ref_{reference_column}"
                ),

                "left"

            )

            .withColumn(

                "RejectReason",

                when(

                    col(
                        f"ref.ref_{reference_column}"
                    ).isNull(),

                    concat(

                        coalesce(
                            col("RejectReason"),
                            lit("")
                        ),

                        lit(
                            f"{column_name} FK Violation; "
                        )

                    )

                )

                .otherwise(
                    col("RejectReason")
                )

            )

            .drop(
                f"ref_{reference_column}"
            )

        )

    return df

# COMMAND ----------

def refresh_rejection_flag(df):

    return (
        df
        .withColumn(
            "_IsRejected",
            when(
                length(trim(col("RejectReason"))) > 0,
                lit(True)
            ).otherwise(
                lit(False)
            )
        )
    )

# COMMAND ----------

from pyspark.sql.functions import col

def split_valid_reject(df):

    valid_df = (
        df
        .filter(
            col("_IsRejected") == False
        )
    )

    reject_df = (
        df
        .filter(
            col("_IsRejected") == True
        )
    )

    return (
        valid_df,
        reject_df
    )

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

def deduplicate_scd_batch(
    valid_df,
    scd_config
):

    natural_key = scd_config[
        "natural_key"
    ]

    sequence_column = scd_config[
        "sequence_column"
    ]

    window_spec = (

        Window

        .partitionBy(
            *natural_key
        )

        .orderBy(
            col(sequence_column).desc()
        )

    )

    dedup_df = (

        valid_df

        .withColumn(
            "_rn",
            row_number().over(
                window_spec
            )
        )

        .filter(
            col("_rn") == 1
        )

        .drop("_rn")

    )

    return dedup_df

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

def apply_scd2(
    valid_df,
    source_name,
    scd_config
):

    source_config = get_source_config(
        source_name
    )

    silver_table = source_config[
        "silver_table"
    ]

    target_table = (
        f"capstone_project.silver_layer.{silver_table}"
    )

    natural_key = scd_config[
        "natural_key"
    ]

    tracked_columns = scd_config[
        "tracked_columns"
    ]

    surrogate_key = scd_config[
        "surrogate_key"
    ]

    # -----------------------------
    # INITIAL LOAD
    # -----------------------------

    if not spark.catalog.tableExists(
        target_table
    ):

        window_spec = Window.orderBy(
            natural_key[0]
        )

        return (

            valid_df

            .withColumn(
                surrogate_key,
                row_number().over(
                    window_spec
                )
            )

            .withColumn(
                "EffectiveDate",
                current_timestamp()
            )

            .withColumn(
                "ExpiryDate",
                lit(None).cast(
                    "timestamp"
                )
            )

            .withColumn(
                "IsCurrent",
                lit(True)
            )

        )

    # -----------------------------
    # INCREMENTAL LOAD
    # -----------------------------

    # -----------------------------
    # INCREMENTAL LOAD
    # -----------------------------

    src = valid_df.alias("src")

    tgt = (
        spark.table(target_table)
        .filter(col("IsCurrent") == True)
        .alias("tgt")
    )

    join_condition = [
        col(f"src.{nk}") == col(f"tgt.{nk}")
        for nk in natural_key
    ]

    comparison_df = src.join(
        tgt,
        join_condition,
        "left"
    )

    # -----------------------------
    # Detect Changes
    # -----------------------------

    change_condition = None

    for column_name in tracked_columns:

        condition = (
            coalesce(col(f"src.{column_name}").cast("string"), lit(""))
            !=
            coalesce(col(f"tgt.{column_name}").cast("string"), lit(""))
        )

        if change_condition is None:
            change_condition = condition
        else:
            change_condition = change_condition | condition

    # -----------------------------
    # Changed Records
    # -----------------------------
    print("Tracked Columns :", tracked_columns)
    changed_df = (
        comparison_df
        .filter(change_condition)
        .select("src.*")
    )
    # -----------------------------
    # New Records
    # -----------------------------

    new_df = (
        comparison_df
        .filter(col(f"tgt.{natural_key[0]}").isNull())
        .select("src.*")
    )

    # -----------------------------
    # Existing Current Records
    # -----------------------------

    current_df = (
        tgt.select("*")
    )

    # -----------------------------
    # Unchanged Records
    # -----------------------------

    unchanged_df = (
        current_df.alias("t")
        .join(
            changed_df.alias("c"),
            natural_key,
            "left_anti"
        )
        .select("t.*")
    )

    # -----------------------------
    # Expire Changed Records
    # -----------------------------

    expired_df = (
        current_df.alias("t")
        .join(
            changed_df.alias("c"),
            natural_key,
            "inner"
        )
        .select("t.*")
        .withColumn("IsCurrent", lit(False))
        .withColumn("ExpiryDate", current_timestamp())
    )

    # -----------------------------
    # Surrogate Key
    # -----------------------------

    max_sk = (
        spark.table(target_table)
        .agg(max(surrogate_key))
        .collect()[0][0]
    )

    if max_sk is None:
        max_sk = 0

    window_spec = Window.orderBy(natural_key[0])

    insert_df = (
        changed_df
        .unionByName(new_df)
        .withColumn(
            surrogate_key,
            row_number().over(window_spec) + max_sk
        )
        .withColumn(
            "EffectiveDate",
            current_timestamp()
        )
        .withColumn(
            "ExpiryDate",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "IsCurrent",
            lit(True)
        )
    )

    # -----------------------------
    # Final Output
    # -----------------------------

    final_df = (
        unchanged_df
        .unionByName(expired_df)
        .unionByName(insert_df)
    )
    print("Changed Records :", changed_df.count())
    print("New Records     :", new_df.count())

    return final_df

# COMMAND ----------

def apply_scd_if_required(
    valid_df,
    source_name
):

    scd_config = get_scd_config(
        source_name
    )

    if not scd_config:

        print(
            f"{source_name} : No SCD Configuration Found"
        )

        return valid_df

    scd_type = scd_config.get(
        "scd_type"
    )

    if scd_type == 2:

        print(
            f"{source_name} : SCD Type 2 Detected"
        )

        valid_df = deduplicate_scd_batch(
            valid_df,
            scd_config
        )

        valid_df = apply_scd2(
            valid_df,
            source_name,
            scd_config
        )

    return valid_df

# COMMAND ----------

def write_silver(
    df,
    source_name
):

    config = get_source_config(
        source_name
    )

    silver_table = config[
        "silver_table"
    ]

    partition_column = (

        config

        .get("incremental_config", {})

        .get("partition_column")

    )

    writer = (

        df.write

        .format("delta")

        .mode("append")

    )

    if (

        partition_column

        and

        partition_column in df.columns

    ):

        print(
            f"Partitioning by : {partition_column}"
        )

        writer = writer.partitionBy(
            partition_column
        )

    writer.saveAsTable(

        f"capstone_project.silver_layer.{silver_table}"

    )

    print(
        f"{silver_table} loaded successfully"
    )

# COMMAND ----------

def write_rejects(
    df,
    source_name
):

    config = get_source_config(
        source_name
    )

    reject_table = config[
        "reject_table"
    ]

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            f"capstone_project.silver_layer.{reject_table}"
        )
    )

    print(
        f"{reject_table} loaded successfully"
    )

# COMMAND ----------

# Cell 19 — Process Silver
from pyspark.sql.functions import current_timestamp
def process_silver(source_name):

    audit = start_audit(
        layer="Silver",
        object_name=source_name
    )

    try:

        # -----------------------------
        # Read Bronze
        # -----------------------------

        bronze_df = read_bronze(source_name)

        rows_read = bronze_df.count()

        # -----------------------------
        # Prepare Source
        # -----------------------------

        bronze_df = prepare_source(
            bronze_df,
            source_name
        )

        bronze_df = apply_schema(
            bronze_df,
            source_name
        )
        bronze_df = apply_incremental_filter(bronze_df,source_name)

        # -----------------------------
        # Initialize Reject Columns
        # -----------------------------

        bronze_df = initialize_rejection_columns(
            bronze_df
        )

        # -----------------------------
        # Mandatory Validation
        # -----------------------------

        bronze_df = validate_mandatory_columns(
            bronze_df,
            source_name
        )

        # -----------------------------
        # Primary Key Validation
        # -----------------------------

        bronze_df = validate_primary_key(
            bronze_df,
            source_name
        )

        # -----------------------------
        # Business Rule Validation
        # -----------------------------

        if get_validation_rules(source_name):

            bronze_df = validate_business_rules(
                bronze_df,
                source_name
            )

        # -----------------------------
        # Foreign Key Validation
        # -----------------------------

        column_metadata = get_column_metadata(source_name)

        has_fk = any(
            metadata.get("key_type") == "FK"
            for metadata in column_metadata.values()
        )

        if has_fk:

            bronze_df = validate_foreign_keys(
                bronze_df,
                source_name
            )

        # -----------------------------
        # Refresh Reject Flag
        # -----------------------------

        bronze_df = refresh_rejection_flag(
            bronze_df
        )

        # -----------------------------
        # Split Valid / Reject
        # -----------------------------

        valid_df, reject_df = split_valid_reject(
            bronze_df
        )
        valid_df, reject_df = split_valid_reject(bronze_df)

        valid_df = valid_df.withColumn(
        "_ProcessedTimestamp",
        current_timestamp()
        )

        # -----------------------------
        # Apply SCD if Required
        # -----------------------------

        if get_scd_config(source_name):

            valid_df = apply_scd_if_required(
                valid_df,
                source_name
            )

       
        # -----------------------------
        # Cache Before Counting
        # -----------------------------

        

        rows_written = valid_df.count()
        rows_rejected = reject_df.count()

        # -----------------------------
        # Write Silver
        # -----------------------------

        write_silver(
            valid_df,
            source_name
        )

        # -----------------------------
        # Write Rejects
        # -----------------------------

        if rows_rejected > 0:

            write_rejects(
                reject_df,
                source_name
            )

        # -----------------------------
        # Audit
        # -----------------------------

        audit = complete_audit(
            audit,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected
        )

        write_audit(audit)

        print(
            f"{source_name} Silver Load Completed"
        )

    except Exception as e:

        audit = complete_audit(
            audit,
            rows_read=0,
            rows_written=0,
            rows_rejected=0,
            status="FAILED",
            error_message=str(e)
        )

        write_audit(audit)

        print(
            f"{source_name} Failed : {str(e)}"
        )

        raise

# COMMAND ----------

# Run all active Silver sources

for source_name in get_active_source_names():

    print("\n" + "=" * 60)
    print(f"Processing Silver: {source_name}")
    print("=" * 60)

    process_silver(source_name)