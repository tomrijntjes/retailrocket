import pytest

from src.config import spark
from src.top_customers import enrich, transform

def test_enrich():
    """
    +-------------+-----------------+----------+----+
    |    timestamp|timestamp_seconds|      date|week|
    +-------------+-----------------+----------+----+
    |1433221332117| 1.433221332117E9|2015-06-02|  23|
    |            0|              0.0|1970-01-01|   1|
    |2866442700000|      2.8664427E9|2060-10-31|  45|
    +-------------+-----------------+----------+----+
    """
    test_df = spark.createDataFrame(
        data=[
        [1433221332117],[0],[2866442700000]],
        schema=['timestamp']
    )
    enriched_test = enrich(test_df).collect()
    enriched_test
    assert enriched_test[0].week == "23"
    #assert enriched_test[1].date == "1970-01-01"
    assert enriched_test[2].week == "45"

def test_transform():
    pass
