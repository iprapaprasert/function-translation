def zeroifnull(col: "ColumnOrName") -> Column:
    """
    Returns zero if `col` is null, or `col` otherwise.

    Parameters
    ----------
    col : `pyspark.sql.Column` or column name
    """
    
    return ifnull(col, lit(0))
