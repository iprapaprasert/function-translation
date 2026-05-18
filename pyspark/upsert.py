def upsert(df: "DataFrame", table: str, mergeKeys: list[str]) -> None:
    """
    Upserts a DataFrame into a Delta table using specified merge keys.

    Performs an upsert operation by matching records on the provided keys:
    - Matching records are updated with the new values.
    - Non-matching records are inserted as new rows.

    Parameters
    ----------
    df : `DataFrame`
        A DataFrame containing the data to upsert.
    table : string
        The name of the Delta table.
    mergeKeys : column name
        A list of column names to use as the merge condition. 
        These columns must exist in both the Delta table and the DataFrame.
    
    Returns
    -------
    None
    """
    path = os.path.join("Tables", table)
    delta_table = DeltaTable.forPath(spark, path)
    
    merge_condition = " AND ".join(
        [f"target.{key} = source.{key}" for key in mergeKeys]
    )
    
    delta_table.alias("target") \
        .merge(
            df.alias("source"),
            merge_condition
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
