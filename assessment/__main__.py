from pyspark.sql import functions as sf
from pyspark.testing.utils import assertDataFrameEqual   
from pyspark import SparkConf

conf = SparkConf()
conf.set("spark.python.worker.faulthandler.enabled", "true")

from assessment.helpers import get_spark
from assessment.transform import compute

if __name__ == "__main__":

    spark = get_spark()

    edges = spark.read.json(
        "data/edges.jsonl",
        """
        parent STRING,
        child STRING
        """,
    )

    output = compute(edges)

    output.write.mode("overwrite").json("data/output.jsonl")

    expected = spark.read.json(
        "data/solution.jsonl",
        """
        node STRING,
        path STRING,
        depth LONG,
        descendants LONG,
        is_root BOOLEAN,
        is_leaf BOOLEAN
        """,
    )

    assertDataFrameEqual(output, expected, checkRowOrder=False)

    print("Success!")
