from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ITEMS = ["510350", "510261", "510349", "630162", "630156",
         "630159", "640219", "640222", "640243"]


def clean_quotes(df: DataFrame) -> DataFrame:
    """Keep in-scope items with valid, positive prices."""
    return df.filter(
        (F.col("ITEM_ID").isin(ITEMS))
        & (F.col("VALIDITY").isin(3, 4))
        & (F.col("PRICE") > 0)
        & (F.col("BASE_PRICE") > 0)
    )
