def xtile(
    df: DataFrame, 
    newvar: str, 
    x: "ColumnOrName", 
    nquantiles: int=2, 
    weight: "ColumnOrName"=lit(1)
) -> DataFrame:
    """
    Create variable containing quantile categories.

    Parameters
    ----------
    df : `pyspark.sql.DataFrame`
        Dataframe
    newvar : str
        New variable name
    x : `pyspark.sql.Column` or column name
        Numeric column to categorize.
    nquantiles : int
        Number of quantiles; default is `nquantiles(2)`
    weight : `pyspark.sql.Column` or column name
        The weight to be attached to each observation.

    Returns
    -------
    `pyspark.sql.DataFrame`
        A given DataFrame with new variable containing the percentiles of `col`
    """
    tempdf = df.select(x, weight)
    
    N = tempdf.select(sum(weight)).collect()[0][0]
    tempdf = tempdf \
        .withColumn("W", sum(weight).over(Window.orderBy(x))) \
        .withColumn("n", row_number().over(Window.orderBy(x)))

    ps = [100 * k / nquantiles for k in range(1, nquantiles)]
    cutoffs = []
    for p in ps:
        P = N*p/100

        # find the first index i such that W_i > P
        row_i = tempdf.filter(col("W") > lit(P)).sort(col("n")).first()
        i = row_i["n"]
        W_i = row_i["W"]
        x_i = row_i[x]
        
        ## h = i-1 
        row_h = tempdf.filter(col("n") == i-1).first()
        W_h = row_h["W"]
        x_h = row_h[x]
        
        if W_h == P:
            cutoff = (x_h + x_i) / 2
        else:
            cutoff = x_i
        cutoffs.append(cutoff)
    
    # bucketizer
    expr = when(col(x) <= cutoffs[0], 1)
    for i in range(1, len(cutoffs)):
        expr = expr.when((col(x) > cutoffs[i-1]) & (col(x) <= cutoffs[i]), i + 1)
    expr = expr.otherwise(len(cutoffs) + 1)

    return df.withColumn(newvar, expr)
