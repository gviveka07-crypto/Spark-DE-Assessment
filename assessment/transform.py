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
        df = df.union(new_nodes)
    edges_cache = edges.cache()
    df_desc = df.alias("d").join(edges_cache.alias("e"), F.col("d.node") == F.col("e.parent"), "left") \
                    .groupBy("d.node").agg(F.count("e.child").alias("descendants"))
    df = df.join(df_desc, df.node == df_desc.node, "left").drop(df_desc.node)
    df = df.withColumn("descendants", F.coalesce(F.col("descendants"), F.lit(0)))

    df = df.withColumn("is_root", F.col("node") == F.lit(root_node))
    df = df.withColumn("is_leaf", F.col("descendants") == 0)

    df = df.select("node", "path", "depth", "descendants", "is_root", "is_leaf")

    return df

    """The assessment logic

    This function is where you will define the logic for the assessment. The
    function will receive a dataframe of edges representing a hierarchical structure,
    and return a dataframe of nodes containing structural information describing the
    hierarchy.

    Consider the tree structure below:
    A
    |-- B
    |   `-- C
    |-- D
    `-- E
        |-- F
        `-- G


    Parameters
    ----------
    edges: DataFrame
        A dataframe of edges representing a hierarchical structure. For the example
        above, the dataframe would be:

        | parent | child |
        |--------|-------|
        | A      | B     |
        | B      | C     |
        | A      | D     |
        | A      | E     |
        | E      | F     |
        | E      | G     |

    Returns
    -------
    DataFrame
        A dataframe of nodes with the following columns:
            - node: The node of the hierarchy
            - path: The path to the node from the root
            - depth: The depth of the node in the hierarchy
            - descendents: The number of descendents of the node
            - is_root: Whether the node is the root of the hierarchy
            - is_leaf: Whether the node is a leaf of the hierarchy

        The output dataframe for the example above would look like:

        | node | path  | depth | descendents | is_root | is_leaf |
        |------|-------|-------|-------------|---------|---------|
        | A    | A     | 0     | 6           | true    | false   |
        | B    | A.B   | 1     | 1           | false   | false   |
        | C    | A.B.C | 2     | 0           | false   | true    |
        | D    | A.D   | 1     | 0           | false   | true    |
        | E    | A.E   | 1     | 2           | false   | false   |
        | F    | A.E.F | 2     | 0           | false   | true    |
        | G    | A.E.G | 2     | 0           | false   | true    |

    """
    ...
