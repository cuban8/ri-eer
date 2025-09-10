# neighbor_ct_graphframe.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F
from graphframes import GraphFrame
# import pandas as pd
from pyspark import StorageLevel
import numpy as np

from src.utils import constant_graphframe
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    TimestampType,
    DateType,
    LongType,
    DoubleType,
)


# --------------------------------------------------------------------------------------
# Initialize Spark session (if standalone)
# --------------------------------------------------------------------------------------
spark = SparkSession.builder.appName("GraphFramesExperiment").getOrCreate()


# --------------------------------------------------------------------------------------
# Neighbor high-risk count features
# --------------------------------------------------------------------------------------
def get_neighbor_ct_feature_graphframe(spark, g_frame, vintage_driver_trxn, vintage_dt, window_sz):
    """
    Calculate neighbor count features using only GraphFrame/Spark operations.
    """

    # Get the list of entity IDs we are interested in
    entity_ids = vintage_driver_trxn.select("EXT_ENTITYID").distinct()

    # Nodes flagged high-risk from either side of trxn
    high_risk_entities = (
        vintage_driver_trxn.filter(F.col("entity_vhrg_flag") == 1)
        .select(F.col("EXT_ENTITYID").alias("id"))
        .withColumn("is_high_risk", F.lit(1))
    )

    high_risk_counterparties = (
        vintage_driver_trxn.filter(F.col("cp_vhrg_flag") == 1)
        .select(F.col("COUNTER_PARTY_ENTITYID").alias("id"))
        .withColumn("is_high_risk", F.lit(1))
    )

    # Combine entity + counterparty high-risk nodes
    high_risk_nodes = (
        high_risk_entities.union(high_risk_counterparties).distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"[PERSIST] High risk nodes: {high_risk_nodes.count()}")

    # Attach high-risk flag to all vertices (default=0 if not present)
    all_vertices_with_risk = (
        g_frame.vertices
        .join(high_risk_nodes, on="id", how="left")
        .fillna(0, subset=["is_high_risk"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Join edges with source nodes
    edges_with_src = (
        g_frame.edges
        .join(all_vertices_with_risk, g_frame.edges.src == all_vertices_with_risk.id, "left")
        .select(
            F.col("src"),
            F.col("dst"),
            F.col("is_high_risk").alias("src_is_high_risk"),
        )
    )

    # Join edges with destination nodes (risk flag)
    edges_with_both = (
        edges_with_src
        .join(all_vertices_with_risk, edges_with_src.dst == all_vertices_with_risk.id, "left")
        .select(
            F.col("src"),
            F.col("dst"),
            F.col("src_is_high_risk"),
            F.col("is_high_risk").alias("dst_is_high_risk"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"[PERSIST] Joined edges with risk attributes: {edges_with_both.count()}")

    # Source-side: count high-risk destination neighbors
    edges_with_risk_src = edges_with_both.withColumn(
        "neighbor_is_high_risk", F.col("dst_is_high_risk")
    )
    high_risk_neighbor_counts_src = (
        edges_with_risk_src
        .groupBy("src")
        .agg(F.sum("neighbor_is_high_risk").alias("high_risk_neighbor_count"))
    )

    # Destination-side: count high-risk source neighbors
    edges_with_risk_dst = edges_with_both.withColumn(
        "neighbor_is_high_risk", F.col("src_is_high_risk")
    )
    high_risk_neighbor_counts_dst = (
        edges_with_risk_dst
        .groupBy("dst")
        .agg(F.sum("neighbor_is_high_risk").alias("high_risk_neighbor_count"))
    )

    # Combine both src/dst counts
    combined_counts = (
        high_risk_neighbor_counts_src.withColumnRenamed("src", "node")
        .union(
            high_risk_neighbor_counts_dst.withColumnRenamed("dst", "node")
        )
        .groupBy("node")
        .agg(F.sum("high_risk_neighbor_count").alias("high_risk_neighbor_count"))
    )

    # Join back to entity IDs of interest
    result = (
        entity_ids
        .join(combined_counts.withColumnRenamed("node", "EXT_ENTITYID"), on="EXT_ENTITYID", how="left")
        .na.fill(0, subset=["high_risk_neighbor_count"])
    )

    # Final schema formatting
    final_result = (
        result
        .select(
            F.col("EXT_ENTITYID").cast(StringType()),
            F.lit(vintage_dt).cast(DateType()).alias("VINTAGE_DT"),
            F.col("high_risk_neighbor_count").cast(LongType()).alias(f"vhrg_neighbor_ct_{window_sz}m"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # Cleanup intermediates
    print("[UNPERSIST] Cleaning up intermediate DataFrames in neighbor count calculation")
    high_risk_nodes.unpersist()
    all_vertices_with_risk.unpersist()
    edges_with_both.unpersist()

    return final_result