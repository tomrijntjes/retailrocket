from pyspark.sql.functions import to_date, from_unixtime, col, date_format, dense_rank, desc, asc
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from src.config import read_table

def enrich(df: DataFrame) -> DataFrame:
    """
    Add interpreted columns derived from the timestamp
    """
    df = df.withColumn("timestamp_seconds", col("timestamp")/1000)
    df = df.withColumn("date", to_date(from_unixtime("timestamp_seconds")))
    df = df.withColumn("week", date_format(to_date(from_unixtime("timestamp_seconds")),"w"))
    return df

def transform(df: DataFrame) -> DataFrame:
    """Weekly top five visitors."""
    grouped = df.where(df.event == "view").groupby("week", "visitorid").count().select("week", "visitorid", col('count').alias('n'))
    grouped_ranked = grouped.withColumn("rank", dense_rank().over(Window.partitionBy("week").orderBy(desc("n"))))
    top_five_customers = grouped_ranked.where(col("rank")<6).orderBy(asc("week"), asc("rank"))
    return top_five_customers



if __name__ == "__main__":
    print("[-] Reading and counting pageviews")
    events = read_table("events")
    enriched_events = enrich(events)
    top_five = transform(enriched_events)
    repartitioned = top_five.repartition(1)
    print("[-] Writing to csv...")
    repartitioned.write.mode('overwrite').csv('top_five_visitors.csv')
    print("[+] Goodbye")
