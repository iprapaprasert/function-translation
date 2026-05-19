def calendar(
    start_date: "date", 
    end_date: "date"
) -> DataFrame:
    '''
    Returns a table with a single column named "Date" that contains a contiguous set of dates. 
    The range of dates is from the specified start date to the specified end date, 
    inclusive of those two dates.

    Note:
    You can get min and max date by running
    
    date_range = df.select(
        min("date_column").alias("start_date"),
        max("date_column").alias("end_date")
    ).collect()[0]

    start_date = date_range.start_date
    end_date = date_range.end_date

    Parameters
    ----------
    start_date : date
        Any expression that returns a datetime.date value.
    end_date : date
        Any expression that returns a datetime.date value.
    
    Returns
    -------
    `DataFrame`
        Returns a table with a single column named "Date" containing a contiguous set of dates. 
        The range of dates is from the specified start date to the specified end date, 
        inclusive of those two dates.
    '''
    
    calendar = spark.createDataFrame(
        [(start_date, end_date)], 
        ["start_date", "end_date"]
    ) \
        .select(
            explode(
                sequence(
                    "start_date", 
                    "end_date", 
                    expr("interval 1 day")
                )
            ).alias("Date")
        ) \
        .select(
            "Date",
            year("Date").alias("Year"),
            quarter("Date").alias("Quarter"),
            month("Date").alias("Month"),
            dayofmonth("Date").alias("Day"),
            date_format("Date", "QQQ").alias("QuarterName"),
            date_format("Date", "MMM").alias("MonthName"),
            date_format(col("Date"), "yyyyQQ").cast(IntegerType()).alias("YearQuarter"),
            date_format(col("Date"), "yyyyMM").cast(IntegerType()).alias("YearMonth")
        )
    return calendar
