# util_graphframe.py
# ---------------------------------------------------------------------
# Utility functions for GraphFrame-based feature engineering
# ---------------------------------------------------------------------

from datetime import datetime
import os
import pandas as pd

from pyspark.sql.functions import (
    when, col, concat, explode, regexp_replace, udf, lit, to_date, date_format,
    last_day, broadcast, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType,
    TimestampType, BooleanType, Row
)


# ---------------------------------------------------------------------
# Current time utility (formatted)
# ---------------------------------------------------------------------
def current_time():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------
# Remove prefixes like "ACCT-" from account ID columns
# ---------------------------------------------------------------------
def remove_acct_prefix(trxn_df):
    prefix_check_cols = [
        "ORIG_REMITTER_ACCT_ID",
        "TP_ORIGIN_ACCT_ID",
        "TP_BENEF_ACCT_ID",
        "BENEF_ACCT_ID",
        "SENDING_INSTN_ACCT_ID",
        "RCV_INSTN_ACCT_ID",
    ]

    for c in trxn_df.columns:
        if c in prefix_check_cols:
            trxn_df = trxn_df.withColumn(c, regexp_replace(col(c), "ACCT-", ""))

    return trxn_df


# ---------------------------------------------------------------------
# Write DataFrame to Hive/Delta table with optional partitioning
# ---------------------------------------------------------------------
def write_df_to_table(df, wr_mode, partition_fld, tbl_name):
    print(f"{current_time()}: Writing {df.count()} records to table {tbl_name}")

    if partition_fld is None:
        df.write.mode(wr_mode).saveAsTable(tbl_name)
    else:
        df.write.mode(wr_mode) \
            .option("mergeSchema", "true") \
            .partitionBy(partition_fld) \
            .saveAsTable(tbl_name)