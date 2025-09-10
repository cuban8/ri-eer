# generate_network_feat_graphframe.py
"""
	•	I imported util_graphframe both as a module (from src.utils import util_graphframe) and pulled constant_graphframe from the same package because the code later calls util_graphframe.current_time() and util_graphframe.write_df_to_table(...).
	•	generate_graph_graphframe is imported from centrality_network_graphframe as a placeholder; move that import to the correct module if it actually lives elsewhere in your repo.
	•	The “OLD FUNCTION” block in your screenshot was a scaffold to derive start_date/end_date. I folded that into the script by setting config.start_date = "2023-02-01" and config.end_date = +24 months. Tweak these to match your production Config.
"""
	

import sys
sys.path.insert(0, "../..")

import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

import numpy as np
import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, DoubleType,
    TimestampType, BooleanType, Row,
)

from src.config_graphframe import Config
from src.features.centrality_network_graphframe import get_centrality_feature_graphframe
from src.features.neighbor_ct_graphframe import get_neighbor_ct_feature_graphframe
from src.features.centrality_network_graphframe import generate_graph_graphframe  # if defined there
# If generate_graph_graphframe lives elsewhere, adjust the import accordingly:
# from src.utils.graph_construction_graphframe import generate_graph_graphframe
from src.utils import util_graphframe
from src.utils.util_graphframe import constant_graphframe


# --------------------------------------------------------------------------------------
# Spark session & basic config
# --------------------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("GraphFramesExperiment")
    .getOrCreate()
)

# Checkpointing & shuffle/parallelism tuned for wide shuffles
spark.sparkContext.setCheckpointDir("/dbfs/tmp/spark-checkpoint")
spark.conf.set("spark.sql.shuffle.partitions", 1024)
spark.conf.set("spark.default.parallelism", 1024)

config = Config()


# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------
def safe_unpersist(obj):
    """
    Safely unpersist a DataFrame or a GraphFrame (its vertices/edges).
    """
    try:
        if obj is None:
            return
        if hasattr(obj, "unpersist"):
            obj.unpersist()
        elif hasattr(obj, "vertices") and hasattr(obj, "edges"):
            # GraphFrame: unpersist its components
            if obj.vertices is not None:
                obj.vertices.unpersist()
            if obj.edges is not None:
                obj.edges.unpersist()
            print("[UNPERSIST] Unpersisted GraphFrame components")
        else:
            print("[UNPERSIST] Object doesn't support unpersist")
    except Exception as e:
        print(f"[UNPERSIST] Warning: {e}")


# --------------------------------------------------------------------------------------
# Core feature generation for a single vintage date
# --------------------------------------------------------------------------------------
def generate_network_data_points(spark, driver_df, trxn_df, vintage_dt, lookback_window_list):
    """
    For a single VINTAGE_DT, construct the ego-network subgraph(s) over the specified
    lookback windows and compute:
      - centrality-based features, and
      - neighbor-based (CTT) features.

    Returns a DataFrame keyed by (EXT_ENTITYID, VINTAGE_DT) with feature columns.
    """

    # Driver universe at this vintage date
    vintage_driver = (
        driver_df
        .filter(F.col("VINTAGE_DT") == vintage_dt)
        .select("EXT_ENTITYID", "VINTAGE_DT")
        .distinct()
    )

    # Pre-filter trxn up to vintage_dt (rolling cuts applied per lookback)
    vintage_trxn_upto_v = trxn_df.filter(F.col("POSTING_DATE") <= vintage_dt)

    # For each lookback window, build the graph & compute features; left-join back
    for lookback_window in lookback_window_list:
        lookback_date = vintage_dt - relativedelta(months=lookback_window)

        # Filter and persist transaction slice in-memory to avoid disk churn
        vintage_driver_trxn = (
            vintage_trxn_upto_v
            .filter(F.col("POSTING_DATE") > lookback_date)
            .persist(StorageLevel.MEMORY_ONLY)
        )
        print(
            f"[PERSIST] Filtered transactions for lookback {lookback_window}m:"
            f" {vintage_driver_trxn.count()} rows"
        )

        # Build GraphFrame from the filtered trxn
        g_frame = generate_graph_graphframe(vintage_driver_trxn)

        # Persist graph components (vertices/edges) to speed repeated passes
        g_frame.vertices.persist(StorageLevel.MEMORY_AND_DISK)
        g_frame.edges.persist(StorageLevel.MEMORY_AND_DISK)
        print(
            f"[PERSIST] Persisted graph with {g_frame.vertices.count()} vertices,"
            f" {g_frame.edges.count()} edges"
        )

        # Centrality features (degree/k-core/betweenness/etc., per implementation)
        feat_centrality = get_centrality_feature_graphframe(
            spark, g_frame, vintage_driver_trxn, vintage_dt, lookback_window
        )

        # Neighbor/CTT features (risk propagation, HRG/VHRG neighborhood, etc.)
        feat_neighbor = get_neighbor_ctt_feature_graphframe(
            spark, g_frame, vintage_driver_trxn, vintage_dt, lookback_window
        )

        # Join both feature sets back to the driver universe
        vintage_driver = (
            vintage_driver
            .join(feat_centrality, on=["EXT_ENTITYID", "VINTAGE_DT"], how="left")
            .join(feat_neighbor,   on=["EXT_ENTITYID", "VINTAGE_DT"], how="left")
        )

        # Cleanup this window’s intermediates
        print(f"[CLEANUP] Freeing resources for lookback window {lookback_window}m")
        safe_unpersist(g_frame)
        vintage_driver_trxn.unpersist()

        import gc
        gc.collect()

    return vintage_driver


# --------------------------------------------------------------------------------------
# Configuration of input/output tables & base datasets
# --------------------------------------------------------------------------------------
config.final_tbl = "111507_iridiscovery_ctg_prod_exp.ri_ee.risk_entity.trxn_final_driver_4fcb_v2"
# Historical/dev target if needed:
# config.network_feat = "111507_datasciar_ctg_prod_exp.ri_ee_risk_model_dev.ee_feat_network_feature_pyspark_dev_4fcb_clone"
config.network_feat = (
    "111507_iridiscovery_ctg_prod_exp.ri_ee.risk_entity.ee_feat_network_feature_4fcb_v2.tmp"
)

# Choose a run/vintage horizon
dt = "2024-09-30"

# Driver (EXT_ENTITYID, VINTAGE_DT)
driver = spark.sql(f"""
    SELECT DISTINCT EXT_ENTITYID, VINTAGE_DT
    FROM {config.final_tbl}
    WHERE VINTAGE_DT = '{dt}'
    LIMIT 100000
""")

# Transactions (EXT_ENTITYID -> COUNTER_PARTY_ENTITYID edges with countries & dates)
trxn = spark.sql(f"""
    SELECT DISTINCT
        EXT_ENTITYID,
        combinedCountryCodes,
        COUNTER_PARTY_ENTITYID,
        COUNTER_PARTY_COMBINEDCOUNTRYCODES,
        TRXN_REF_ID,
        VINTAGE_DT,
        POSTING_DATE
    FROM {config.final_tbl}
    LIMIT 500000
""").filter(F.col("EXT_ENTITYID").isNotNull())

# Normalize potential null IDs (keep strings for joins)
trxn = trxn.na.fill({"EXT_ENTITYID": "NaN", "COUNTER_PARTY_ENTITYID": "NaN"})

# --------------------------------------------------------------------------------------
# HRG/VHRG flags (country-based)
# --------------------------------------------------------------------------------------
HRG_VHRG = constant_graphframe.HIGH_RISK_COUNTRIES + constant_graphframe.VERY_HIGH_RISK_COUNTRIES
HRG_VHRG_LIT = F.array([F.lit(c) for c in HRG_VHRG])

trxn = (
    trxn
    .withColumn(
        "entity_hrg_flag",
        F.when(F.size(F.array_intersect(F.col("combinedCountryCodes"), HRG_VHRG_LIT)) > 0, F.lit(1))
         .otherwise(F.lit(0))
    )
    .withColumn(
        "cp_hrg_flag",
        F.when(
            F.size(F.array_intersect(F.col("COUNTER_PARTY_COMBINEDCOUNTRYCODES"), HRG_VHRG_LIT)) > 0,
            F.lit(1),
        ).otherwise(F.lit(0))
    )
)

# --------------------------------------------------------------------------------------
# Vintage windowing: set a start/end horizon (as per your earlier “OLD FUNCTION” logic)
# --------------------------------------------------------------------------------------
start_time = time.time()

# Example horizon based on a pivot start date (adjust to your Config semantics)
vintage_start_dt = "2023-02-01"
config.start_date = vintage_start_dt
config.end_date = (
    (datetime.strptime(vintage_start_dt, "%Y-%m-%d")) + relativedelta(months=24)
).strftime("%Y-%m-%d")

# Pull distinct vintage dates inside the horizon in Spark (not Python)
vintage_dates = (
    driver
    .select("VINTAGE_DT")
    .distinct()
    .filter(
        (F.col("VINTAGE_DT") >= F.lit(config.start_date)) &
        (F.col("VINTAGE_DT") <= F.lit(config.end_date))
    )
    .orderBy("VINTAGE_DT")
    .collect()
)

# 6- and 12-month lookbacks from each vintage
lookback_window_list = [6, 12]

# --------------------------------------------------------------------------------------
# Main loop: iterate vintages, compute features, write to Delta
# --------------------------------------------------------------------------------------
for row in vintage_dates:
    vintage_dt = row.VINTAGE_DT

    print(
        "current time:",
        util_graphframe.current_time(),
        f": getting data points for vintage date {vintage_dt}",
    )

    print(
        f"[PROCESSING] Vintage date {vintage_dt} with"
        f" driver:{driver.count()} rows, trxn:{trxn.count()} rows"
    )

    feat_comb = generate_network_data_points(
        spark, driver, trxn, vintage_dt, lookback_window_list
    )

    print(
        f"[WRITE] Saving {feat_comb.count()} rows to Delta table {config.network_feat}"
    )
    util_graphframe.write_df_to_table(
        feat_comb, mode="append", partition_col="VINTAGE_DT", table_name=config.network_feat
    )

print("Finishing:", time.time() - start_time)