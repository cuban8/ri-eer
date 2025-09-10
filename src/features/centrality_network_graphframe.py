# centrality_network_graphframe.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F
from graphframes import GraphFrame
# import pandas as pd   # optional if needed
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

from pyspark import StorageLevel


# --------------------------------------------------------------------------------------
# Initialize Spark (if running standalone)
# --------------------------------------------------------------------------------------
spark = SparkSession.builder.appName("GraphFramesExperiment").getOrCreate()


# --------------------------------------------------------------------------------------
# Build GraphFrame from transactions
# --------------------------------------------------------------------------------------
def generate_graph_graphframe(vintage_driver_trxn):
    """
    Create GraphFrame directly from Spark DataFrame

    Args:
        vintage_driver_trxn: Spark DataFrame containing transaction data

    Returns:
        GraphFrame object
    """

    # Create vertices DataFrame
    vertices = (
        vintage_driver_trxn
        .select(F.col("EXT_ENTITYID").alias("id"))
        .union(vintage_driver_trxn.select(F.col("COUNTER_PARTY_ENTITYID").alias("id")))
        .filter(~F.col("id").isin(["9999", "NaN", ""]))
        .filter(F.col("id").isNotNull())
        .distinct()
    )

    # Create edges DataFrame
    edges = (
        vintage_driver_trxn
        .select(
            F.col("EXT_ENTITYID").alias("src"),
            F.col("COUNTER_PARTY_ENTITYID").alias("dst")
        )
        .filter(~F.col("src").isin(["9999", "NaN", ""]))
        .filter(~F.col("dst").isin(["9999", "NaN", ""]))
        .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
        .distinct()
    )

    # Create GraphFrame
    g = GraphFrame(vertices, edges)
    print(f"[GRAPH] Created with {g.vertices.count()} vertices, {g.edges.count()} edges")
    return g


# --------------------------------------------------------------------------------------
# Compute centrality features
# --------------------------------------------------------------------------------------
def get_centrality_feature_graphframe(spark, g_frame, vintage_driver_trxn, vintage_dt, window_sz):
    """
    Calculate centrality features using only GraphFrame

    Args:
        spark: SparkSession
        g_frame: GraphFrame object
        vintage_driver_trxn: Spark DataFrame (transactions)
        vintage_dt: current vintage date
        window_sz: lookback window size (months)
    """

    # Extract account IDs for driver universe
    accounts_df = (
        vintage_driver_trxn
        .select(F.col("EXT_ENTITYID").cast("string").alias("EXT_ENTITYID"))
        .distinct()
    )

    # 1. Degree centrality
    degrees = g_frame.degrees.persist(StorageLevel.MEMORY_ONLY)
    print(f"[PERSIST] Persisted degrees: {degrees.count()} rows")

    # 2. PageRank centrality
    pagerank = g_frame.pageRank(resetProbability=0.15, maxIter=5)
    pagerank_vertices = pagerank.vertices.persist(StorageLevel.MEMORY_AND_DISK)
    print(f"[PERSIST] Persisted pagerank vertices: {pagerank_vertices.count()} rows")

    # 3. Network size (connected components)
    print(f"[CRITICAL] Starting connected components with graph size V:{g_frame.vertices.count()}")
    connected_components = (
        g_frame.connectedComponents(checkpointInterval=10)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"[CRITICAL] Connected components completed")

    component_sizes = (
        connected_components
        .groupBy("component")
        .count()
        .withColumnRenamed("count", "network_size")
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # 4. Community detection (label propagation)
    print(f"[CRITICAL] Starting community detection labelPropagation")
    communities = (
        g_frame.labelPropagation(maxIter=5)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    print(f"[CRITICAL] Community detection completed")

    # Community metrics
    edges_with = (
        g_frame.edges
        .join(communities.select("id", "label").withColumnRenamed("label", "src_community"),
              g_frame.edges.src == communities.id, "left")
        .drop("id")
        .join(communities.select("id", "label").withColumnRenamed("label", "dst_community"),
              g_frame.edges.dst == communities.id, "left")
        .drop("id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    intra_community_edges = edges_with.filter(
        F.col("src_community") == F.col("dst_community")
    )

    edges_per_community = (
        intra_community_edges
        .groupBy("src_community")
        .count()
        .withColumnRenamed("count", "edge_count")
    )

    community_metrics = (
        communities
        .groupBy("label")
        .count()
        .withColumnRenamed("count", "community_size")
    )

    community_metrics = (
        community_metrics
        .join(edges_per_community, community_metrics.label == edges_per_community.src_community, "left")
        .drop("src_community")
    )

    # density and avg degree
    community_metrics = (
        community_metrics
        .withColumn("possible_edges", (F.col("community_size") * (F.col("community_size") - 1)) / 2)
        .withColumn("community_density",
                    F.when(F.col("possible_edges") > 0,
                           F.col("edge_count") / F.col("possible_edges")).otherwise(0.0))
        .withColumn("community_avg_degree",
                    F.when(F.col("community_size") > 0,
                           F.col("edge_count") * 2 / F.col("community_size")).otherwise(0.0))
        .na.fill({"edge_count": 0, "community_density": 0.0, "community_avg_degree": 0.0})
    )

    vertices_with_dummy_eigenvector = g_frame.vertices.withColumn("eigenvector_score", F.lit(0.5))

    # Join metrics together
    metrics_df = (
        degrees
        .join(pagerank_vertices.select("id", F.col("pagerank").alias("pagerank_score")), "id", "left")
        .join(connected_components.select("id", "component"), "id", "left")
        .join(component_sizes, "component", "left")
        .join(communities.select("id", "label"), "id", "left")
        .join(community_metrics.select("label", "community_size", "community_density", "community_avg_degree"),
              "label", "left")
        .join(vertices_with_dummy_eigenvector.select("id", "eigenvector_score"), "id", "left")
    )

    # Format result
    result = (
        metrics_df
        .select(
            F.col("id").cast(StringType()).alias("EXT_ENTITYID"),
            F.lit(vintage_dt).cast(DateType()).alias("VINTAGE_DT"),
            F.col("degree").cast(LongType()).alias(f"degree_centrality_{window_sz}m"),
            F.col("pagerank_score").cast(DoubleType()).alias(f"pagerank_centrality_{window_sz}m"),
            F.col("eigenvector_score").cast(DoubleType()).alias(f"eigenvector_centrality_{window_sz}m"),
            F.col("community_size").cast(LongType()).alias(f"community_size_{window_sz}m"),
            F.col("community_density").cast(DoubleType()).alias(f"community_density_{window_sz}m"),
            F.col("community_avg_degree").cast(DoubleType()).alias(f"average_degree_{window_sz}m"),
            F.col("network_size").cast(LongType()).alias(f"network_size_{window_sz}m"),
        )
    )

    # Keep only drivers
    result = (
        result
        .join(accounts_df, on="EXT_ENTITYID", how="inner")
        .persist(StorageLevel.MEMORY_ONLY)
    )

    # Cleanup intermediates
    print("[UNPERSIST] Cleaning up intermediate DataFrames in centrality calculations")
    degrees.unpersist()
    pagerank_vertices.unpersist()
    connected_components.unpersist()
    component_sizes.unpersist()
    communities.unpersist()
    edges_with.unpersist()

    return result