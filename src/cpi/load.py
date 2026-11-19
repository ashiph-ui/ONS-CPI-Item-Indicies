from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from cpi.session import get_spark

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRICE_QUOTES_CSV = str(PROJECT_ROOT / "data/raw/pricequotes/price-quotes-202[34]-*.csv")
PRICE_QUOTES_PARQUET = str(PROJECT_ROOT / "data/parquet/price_quotes")


PRICE_QUOTE_SCHEMA = StructType([
    StructField("QUOTE_DATE",         StringType(),  True),
    StructField("ITEM_ID",            StringType(),  True),
    StructField("ITEM_DESC",          StringType(),  True),
    StructField("VALIDITY",           IntegerType(), True),
    StructField("SHOP_CODE",          StringType(),  True),
    StructField("PRICE",              DoubleType(),  True),
    StructField("INDICATOR_BOX",      StringType(),  True),
    StructField("ORIG_INDICATOR_BOX", StringType(),  True),
    StructField("PRICE_RELATIVE",     DoubleType(),  True),
    StructField("LOG_PRICE_RELATIVE", DoubleType(),  True),
    StructField("STRATUM_WEIGHT",     DoubleType(),  True),
    StructField("STRATUM_TYPE",       IntegerType(), True),
    StructField("START_DATE",         StringType(),  True),
    StructField("END_DATE",           StringType(),  True),
    StructField("REGION",             StringType(),  True),
    StructField("SHOP_TYPE",          IntegerType(), True),
    StructField("SHOP_WEIGHT",        IntegerType(), True),
    StructField("BASE_PRICE",         DoubleType(),  True),
    StructField("BASE_VALIDITY",      IntegerType(), True),
    StructField("STRATUM_CELL",       StringType(),  True),
])


def read_price_quotes(spark: SparkSession, path: str) -> DataFrame:
    """Read price quote CSVs matching a path or glob, using the explicit schema."""
    return spark.read.csv(path, schema=PRICE_QUOTE_SCHEMA, header=True)


def add_date_columns(df: DataFrame) -> DataFrame:
    """Parse QUOTE_DATE from yyyyMM to a date and add year and month columns."""
    return (
        df
        .withColumn("QUOTE_DATE", F.to_date(F.col("QUOTE_DATE"), "yyyyMM"))
        .withColumn("year", F.year(F.col("QUOTE_DATE")))
        .withColumn("month", F.month(F.col("QUOTE_DATE")))
    )


def write_parquet(df: DataFrame, path: str) -> None:
    """Write a DataFrame to Parquet, partitioned by year, replacing any existing output."""
    df.write.mode("overwrite").partitionBy("year").parquet(path)


if __name__ == "__main__":
    spark = get_spark()
    quotes = add_date_columns(read_price_quotes(spark, PRICE_QUOTES_CSV))
    write_parquet(quotes, PRICE_QUOTES_PARQUET)
