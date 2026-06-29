# Databricks notebook source
# MAGIC %run /Workspace/Users/kuttankutt2111@gmail.com/project_capstone/common_utils

# COMMAND ----------

def get_gold_storage():

    framework = get_framework_config()

    catalog = framework["execution_audit_policy"]["catalog"]

    schema = "gold_layer"     # or whatever your Gold schema is

    return catalog, schema

# COMMAND ----------

def read_gold_dimension(dimension_name):

    catalog, schema = get_gold_storage()

    return spark.table(
        f"{catalog}.{schema}.{dimension_name}"
    )

# COMMAND ----------

def get_fact_config(
    fact_name
):

    metadata = load_gold_metadata()

    return metadata[
        "facts"
    ].get(
        fact_name
    )

# COMMAND ----------

def read_silver_for_gold(
    table_name
):

    return spark.table(
        f"capstone_project.silver_layer.{table_name}"
    )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    col
)

def process_dimension(
    dimension_name
):

    audit = start_audit(

        layer="Gold",

        object_name=dimension_name,

        object_type="dimension"

    )

    try:

        config = get_dimension_config(
            dimension_name
        )

        # -------------------------
        # Generated Dimension
        # -------------------------

        if "generated" in config:

            source_table = config[
                "source_table"
            ]

            source_date_column = config[
                "source_date_column"
            ]

            surrogate_key = config[
                "surrogate_key"
            ]

            columns = config[
                "columns"
            ]

            derived_attributes = config.get(
                "derived_attributes"
            )

            df = read_silver_for_gold(
                source_table
            )

            rows_read = df.count()

            df = (

                df

                .select(

                    col(
                        source_date_column
                    ).alias(
                        "Date"
                    )

                )

                .distinct()

            )

            window_spec = Window.orderBy(
                "Date"
            )

            df = df.withColumn(

                surrogate_key,

                row_number().over(
                    window_spec
                )

            )

            if derived_attributes:

                df = apply_derived_columns(

                    df,

                    derived_attributes

                )

            df = df.select(
                *columns
            )

            write_gold_dimension(

                df,

                dimension_name

            )

            rows_written = df.count()

            audit_df = complete_audit(

                audit,

                rows_read=rows_read,

                rows_written=rows_written

            )

            write_audit(
                audit_df
            )

            return df

        # -------------------------
        # Regular Dimension
        # -------------------------

        source_table = config[
            "source_table"
        ]

        columns = config[
            "columns"
        ]

        surrogate_key = config[
            "surrogate_key"
        ]

        natural_key = config[
            "natural_key"
        ]

        df = read_silver_for_gold(
            source_table
        )

        rows_read = df.count()

        df = df.select(
            *columns
        )

        if "deduplicate" in config:

            df = df.distinct()

        if surrogate_key not in df.columns:

            window_spec = Window.orderBy(
                natural_key
            )

            df = df.withColumn(

                surrogate_key,

                row_number().over(
                    window_spec
                )

            )

        final_columns = []

        if surrogate_key not in columns:

            final_columns.append(
                surrogate_key
            )

        final_columns.extend(
            columns
        )

        df = df.select(
            *final_columns
        )

        write_gold_dimension(

            df,

            dimension_name

        )

        rows_written = df.count()

        audit_df = complete_audit(

            audit,

            rows_read=rows_read,

            rows_written=rows_written

        )

        write_audit(
            audit_df
        )

        return df

    except Exception as e:

        audit_df = complete_audit(

            audit,

            rows_read=0,

            rows_written=0,

            status="FAILED",

            error_message=str(e)

        )

        write_audit(
            audit_df
        )

        print(
            f"{dimension_name} Failed : {str(e)}"
        )

        raise

# COMMAND ----------

def write_gold_dimension(
    df,
    dimension_name
):

    (
        df

        .write

        .mode(
            "overwrite"
        )

        .saveAsTable(
            f"capstone_project.gold_layer.{dimension_name}"
        )
    )

    print(
        f"{dimension_name} loaded successfully"
    )

# COMMAND ----------

def write_gold_fact(
    df,
    fact_name
):

    (
        df

        .write

        .format(
            "delta"
        )

        .mode(
            "overwrite"
        )

        .saveAsTable(
            f"capstone_project.gold_layer.{fact_name}"
        )

    )

    print(
        f"{fact_name} loaded successfully"
    )

# COMMAND ----------

def process_fact(
    fact_name
):

    audit = start_audit(

        layer="Gold",

        object_name=fact_name,

        object_type="fact"

    )

    try:

        config = get_fact_config(
            fact_name
        )

        source_table = config[
            "source_table"
        ]

        dimension_links = config.get(
            "dimension_links"
        )

        derived_measures = config.get(
            "derived_measures"
        )

        final_columns = config[
            "final_columns"
        ]

        fact_df = read_silver_for_gold(
            source_table
        )

        rows_read = fact_df.count()

        if dimension_links:

            for (

                dimension_name,

                dimension_config

            ) in dimension_links.items():

                fact_df = apply_dimension_lookup(

                    fact_df,

                    dimension_name,

                    dimension_config

                )

        if derived_measures:

            fact_df = apply_derived_columns(

                fact_df,

                derived_measures,

                round_values=True

            )

        fact_df = fact_df.select(
            *final_columns
        )

        write_gold_fact(

            fact_df,

            fact_name

        )

        rows_written = fact_df.count()

        audit_df = complete_audit(

            audit,

            rows_read=rows_read,

            rows_written=rows_written

        )

        write_audit(
            audit_df
        )

        return fact_df

    except Exception as e:

        audit_df = complete_audit(

            audit,

            rows_read=0,

            rows_written=0,

            status="FAILED",

            error_message=str(e)

        )

        write_audit(
            audit_df
        )

        print(
            f"{fact_name} Failed : {str(e)}"
        )

        raise

# COMMAND ----------

from pyspark.sql.functions import col

def apply_dimension_lookup(
    fact_df,
    dimension_name,
    dimension_config
):

    fact_key = dimension_config[
        "fact_key"
    ]

    dimension_key = dimension_config[
        "dimension_key"
    ]

    surrogate_key = dimension_config[
        "surrogate_key"
    ]

    dimension_df = (

        read_gold_dimension(
            dimension_name
        )

        .select(
            col(dimension_key).alias(
                f"dim_{dimension_key}"
            ),
            col(surrogate_key)
        )

    )

    fact_df = (

        fact_df.alias("fact")

        .join(

            dimension_df.alias("dim"),

            col(
                f"fact.{fact_key}"
            )
            ==
            col(
                f"dim.dim_{dimension_key}"
            ),

            "left"

        )

        .drop(
            f"dim_{dimension_key}"
        )

    )

    return fact_df

# COMMAND ----------

from pyspark.sql.functions import expr, round

def apply_derived_columns(
    df,
    derived_columns,
    round_values = False
):

    for (
        column_name,
        expression
    ) in derived_columns.items():

        if round_values:

            df = df.withColumn(

                column_name,

                round(
                    expr(expression),
                    2
                )

            )

        else:

            df = df.withColumn(

                column_name,

                expr(expression)

            )

    return df

# COMMAND ----------

def get_all_dimensions():

    metadata = load_gold_metadata()

    return metadata[
        "dimensions"
    ]


def get_all_facts():

    metadata = load_gold_metadata()

    return metadata[
        "facts"
    ]

# COMMAND ----------

def process_gold():

    dimensions = get_all_dimensions()

    facts = get_all_facts()

    for dimension_name in dimensions.keys():

        print(
            f"Processing {dimension_name}"
        )

        process_dimension(
            dimension_name
        )

   

    for fact_name in facts.keys():

        print(
            f"Processing {fact_name}"
        )

        process_fact(
            fact_name
        )

    print(
        "Gold Layer Completed"
    )

# COMMAND ----------

process_gold()