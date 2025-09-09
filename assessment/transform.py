from pyspark.sql import DataFrame, functions as F


def compute(edges: DataFrame) -> DataFrame:
   
    spark = edges.sparkSession

    edge_list = [(row["parent"], row["child"]) for row in edges.collect()]

    children_map = {}
    parents = set()
    all_nodes = set()

    for p, c in edge_list:
        children_map.setdefault(p, []).append(c)
        parents.add(p)
        all_nodes.add(p)
        all_nodes.add(c)


    child_nodes = {c for _, c in edge_list}
    root = list(all_nodes - child_nodes)[0]

    results = []

    def dfs(node, path, depth):
        kids = children_map.get(node, [])
        descendants = 0

        for k in kids:
            d = dfs(k, path + "." + k, depth + 1)
            descendants += 1 + d

        results.append(
            (
                node,
                path,
                depth,
                descendants,
                node == root,
                len(kids) == 0,
            )
        )
        return descendants

    # Lets start recursion at root
    dfs(root, root, 0)

    # finally create Spark DataFrame from  results as per the requested schema
    df = spark.createDataFrame(
        results,
        ["node", "path", "depth", "descendants", "is_root", "is_leaf"],
    )

    return df

