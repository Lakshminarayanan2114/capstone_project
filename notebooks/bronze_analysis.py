# Databricks notebook source
# MAGIC %run /Workspace/Users/kuttankutt2111@gmail.com/project_capstone/common_utils

# COMMAND ----------

# Cell 2 — dedicated bronze write function
def writebronze_layer(df, catalog, schema, table_name, mode="append"):

    full_table = f"{catalog}.{schema}.{table_name}"

    (
        df.write
        .format("delta")
        .mode(mode)
        .saveAsTable(full_table)
    )

    print(f"{table_name} loaded successfully")






# COMMAND ----------

# Cell 3 — Bronze Load Function

def load_bronze(source_name):

    audit = start_audit(
        layer="Bronze",
        object_name=source_name
    )

    try:

        config = get_source_config(source_name)

        # -------------------------------------------------------
        # Read Source
        # -------------------------------------------------------

        df = read_source(source_name)
        print("Schema immediately after read_source()")
        df.printSchema()
        rows_read = df.count()

        print(f"[{source_name}] Rows before filter : {rows_read}")

        # -------------------------------------------------------
        # Incremental Watermark Filter
        # -------------------------------------------------------

        if config.get("ingestion_type") == "WatermarkIncremental":

            incr_config = get_incremental_config(source_name)

            watermark_column = incr_config["watermark_column"]

            bronze_table = (
                f"capstone_project.bronze_layer.{config['bronze_table']}"
            )

            if spark.catalog.tableExists(bronze_table):

                last_watermark = (
                    spark.table(bronze_table)
                    .agg(max(col(watermark_column)))
                    .collect()[0][0]
                )

                print(f"[{source_name}] Last Watermark : {last_watermark}")

                if last_watermark is not None:

                    df = df.filter(
                        col(watermark_column) > lit(last_watermark)
                    )

                    print(f"[{source_name}] Incremental filter applied")

        rows_after_filter = df.count()

        print(f"[{source_name}] Rows after filter : {rows_after_filter}")

        # -------------------------------------------------------
        # Bronze Audit Columns
        # -------------------------------------------------------

        # Uncomment when required
        df = add_bronze_audit_columns(df)
        print(df.columns)

        rows_written = rows_after_filter

        print(f"[{source_name}] Rows to write : {rows_written}")
        print("Schema before write")
        df.printSchema()

        # -------------------------------------------------------
        # Write Bronze
        # -------------------------------------------------------

        writebronze_layer(
            df,
            catalog="capstone_project",
            schema="bronze_layer",
            table_name=config["bronze_table"],
            mode="append"
        )

        print(f"[{source_name}] Bronze write completed.")

        # -------------------------------------------------------
        # Audit
        # -------------------------------------------------------

        audit = complete_audit(
            audit=audit,
            rows_read=rows_read,
            rows_written=rows_written,
            status="SUCCESS"
        )

        write_audit(audit)

        print(f"[{source_name}] Audit write completed.")

        print(
            f"[{source_name}] Bronze Load Completed - {rows_written} rows written"
        )

    except Exception as e:

        print("Original Exception:")
        print(type(e))
        print(e)

        audit = complete_audit(
            audit=audit,
            rows_read=0,
            rows_written=0,
            status="FAILED",
            error_message=str(e)
        )

        write_audit(audit)

        raise

# COMMAND ----------

# Cell 4 — run all active sources
for source_name in get_active_source_names():
    print(f"\n{'='*50}")
    print(f"Processing: {source_name}")
    print(f"{'='*50}")
    load_bronze(source_name)

# COMMAND ----------

print("===== DataFrame Schema =====")


print("===== Delta Table Schema =====")
spark.table(
    "capstone_project.bronze_layer.bronze_exchange_rates"
).printSchema()

# COMMAND ----------

spark.table("capstone_project.bronze_layer.bronze_customers").printSchema()

# COMMAND ----------

print(get_incremental_config("Customers"))

# COMMAND ----------

from pyspark.sql.functions import max

display(
    spark.table("capstone_project.bronze_layer.bronze_customers")
         .agg(max("LastUpdated").alias("CurrentWatermark"))
)

# COMMAND ----------

display(
    read_source("Customers")
        .agg(max("LastUpdated").alias("SourceMaxWatermark"))
)