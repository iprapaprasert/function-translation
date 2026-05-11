from pyspark.sql.functions import *
from functools import reduce

def venn(
    named_dfs: dict, 
    id_col: str
) -> DataFrame:
    """
    Count unique citizens in every combination (Venn-Euler) across multiple dataframes.

    Parameters
    ----------
    named_dfs ({str, DataFrame}) : {label, dataframe}, each must contain `id_col`.
    id_col (str) : Column name of the citizen ID, must be in the every dataframe.
    
    Returns
    -------
    `pyspark.sql.DataFrame`
        Table summarizing number of citizens in each combination.
    """

    prepared = {}
    for name, df in named_dfs.items():
        prepared[name] = df.select(id_col) \
            .dropna(subset=id_col) \
            .dropDuplicates() \
            .withColumn(name, lit(1))

    joined = reduce(
        lambda df1, df2: df1.join(df2, id_col, "full"), 
        prepared.values()
    )

    group_cols = [col for col in joined.columns if col != id_col]
    grouped = joined.groupBy(group_cols) \
        .agg(count_distinct(id_col).alias("n"))

    return grouped
