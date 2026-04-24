def tabulate_oneway(
    df: DataFrame, 
    varname: str, 
    missing: bool=False
) -> DataFrame:
    """
    One-way table of frequencies

    Parameters
    ----------
    df : `pyspark.sql.DataFrame`
        Dataframe
    varname : str
        Variable name
    missing : bool
        Requests that missing values be treated like other values in calculations of counts, percentages, and other statistics.
    
    Returns
    -------
    `pyspark.sql.DataFrame`
         A one-way table of frequency counts
    """
    if not missing:
        df = df.filter(col(varname).isNotNull())

    tab = df.groupBy(varname).agg(count("*").alias("Freq"))
    
    tab = tab \
        .withColumn("Percent", round(col("Freq") / sum(col("Freq")).over(Window.partitionBy()) * 100, 2)) \
        .withColumn("Cum", round(sum("Percent").over(Window.orderBy(col(varname).asc_nulls_last())), 2))
    
    total_row = spark.createDataFrame([("Total", df.count(), 100.00)], [varname, "Freq.", "Percent"])

    tab = tab.select(
        col(varname).cast("string").alias(varname),
        col("Freq").alias("Freq."),
        "Percent",
        col("Cum").alias("Cum.")
    )

    return tab.unionByName(total_row, allowMissingColumns=True).show()
