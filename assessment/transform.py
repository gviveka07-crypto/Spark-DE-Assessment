from pyspark.sql import DataFrame


def compute(edges: DataFrame) -> DataFrame:
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
