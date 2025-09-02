from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

def compute(edges: DataFrame) -> DataFrame:
    spark = edges.sparkSession

    parents = edges.select(F.col("parent").alias("node"))
    children = edges.select(F.col("child").alias("node"))
    nodes = parents.union(children).distinct()

    child_nodes = children.select("node").distinct()
    root_node = parents.join(child_nodes, on="node", how="left_anti").collect()[0]["node"]

    df = spark.createDataFrame([(root_node, root_node, 0)], ["node", "path", "depth"])

    while True:
        joined = df.join(edges, df.node == edges.parent, "inner") \
                   .select(edges.child.alias("node"),
                           (df.path + "." + edges.child).alias("path"),
                           (df.depth + 1).alias("depth"))
        new_nodes = joined.join(df, on="node", how="left_anti")
        if new_nodes.count() == 0:
            break
        df = df.union(new_nodes)  # <- fixed typo here ("new_n)odes" -> "new_nodes")

    edges_cache = edges.cache()
    df_desc = df.alias("d").join(edges_cache.alias("e"), F.col("d.node") == F.col("e.parent"), "left") \
                    .groupBy("d.node").agg(F.count("e.child").alias("descendants"))
    df = df.join(df_desc, df.node == df_desc.node, "left").drop(df_desc.node)
    df = df.withColumn("descendants", F.coalesce(F.col("descendants"), F.lit(0)))

    df = df.withColumn("is_root", F.col("node") == F.lit(root_node))
    df = df.withColumn("is_leaf", F.col("descendants") == 0)


    df = df.select("node", "path", "depth", "descendants", "is_root", "is_leaf")

    return df

 
