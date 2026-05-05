from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VOLUME_PATH = "/Volumes/claude_aidevkit/raw/source_obt_file/"

# Exact schema of rows emitted by _parse_excel; must match the Excel column names
# Auto Loader (binaryFile) picks up new .xlsx files; mapInPandas parses each one.
_raw_schema = StructType([
    StructField("Order_ID", StringType(), True),
    StructField("Order_Date", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Customer_Name", StringType(), True),
    StructField("Customer_Email", StringType(), True),
    StructField("Customer_Segment", StringType(), True),
    StructField("Product_ID", StringType(), True),
    StructField("Product_Name", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Unit_Price", DoubleType(), True),
    StructField("Quantity", LongType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Gross_Sales", DoubleType(), True),
    StructField("Discount_Amount", DoubleType(), True),
    StructField("Net_Sales", DoubleType(), True),
    StructField("_source_file", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])


def _parse_excel(batch_iter):
    """mapInPandas callback: receives Iterator[pd.DataFrame], yields Iterator[pd.DataFrame].

    Each row in each incoming batch represents one .xlsx file (path + binary content).
    """
    import io
    from datetime import datetime, timezone

    import pandas as pd

    field_names = [f.name for f in _raw_schema.fields]
    numeric_float = ("Unit_Price", "Discount", "Gross_Sales", "Discount_Amount", "Net_Sales")

    for batch in batch_iter:
        results = []
        for _, row in batch.iterrows():
            df = pd.read_excel(io.BytesIO(bytes(row["content"])), engine="openpyxl")
            df["_source_file"] = row["path"]
            df["_ingested_at"] = datetime.now(timezone.utc)
            for col in numeric_float:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            if "Quantity" in df.columns:
                df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").astype("Int64")
            results.append(df[field_names])
        if results:
            yield pd.concat(results, ignore_index=True)
        else:
            yield pd.DataFrame({n: pd.Series(dtype="object") for n in field_names})


def _obt_stream():
    """Return a streaming DataFrame: one parsed row per OBT source row.

    Auto Loader watches VOLUME_PATH for new .xlsx files; each pipeline run
    only processes files not yet seen (checkpoint managed by SDP per flow).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.xlsx")
        .option("recursiveFileLookup", "true")
        .load(VOLUME_PATH)
        .mapInPandas(_parse_excel, schema=_raw_schema)
    )


# ── Streaming tables + append_flow ────────────────────────────────────────────

dp.create_streaming_table(
    "claude_aidevkit.bronze.bronze_order",
    comment="Raw order records — appended incrementally as new OBT files arrive",
    cluster_by=["order_date"],
)


@dp.append_flow(target="claude_aidevkit.bronze.bronze_order")
def ingest_bronze_order():
    return (
        _obt_stream()
        .select(
            F.col("Order_ID").alias("order_id"),
            F.col("Order_Date").alias("order_date"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )


dp.create_streaming_table(
    "claude_aidevkit.bronze.bronze_customer",
    comment="Raw customer records — appended incrementally as new OBT files arrive",
    cluster_by=["customer_id"],
)


@dp.append_flow(target="claude_aidevkit.bronze.bronze_customer")
def ingest_bronze_customer():
    return (
        _obt_stream()
        .select(
            F.col("Customer_ID").alias("customer_id"),
            F.col("Customer_Name").alias("customer_name"),
            F.col("Customer_Email").alias("email"),
            F.col("Customer_Segment").alias("customer_segment"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )


dp.create_streaming_table(
    "claude_aidevkit.bronze.bronze_product",
    comment="Raw product records — appended incrementally as new OBT files arrive",
    cluster_by=["product_id"],
)


@dp.append_flow(target="claude_aidevkit.bronze.bronze_product")
def ingest_bronze_product():
    return (
        _obt_stream()
        .select(
            F.col("Product_ID").alias("product_id"),
            F.col("Product_Name").alias("product_name"),
            F.col("Category").alias("category"),
            F.col("Unit_Price").alias("unit_price"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )


dp.create_streaming_table(
    "claude_aidevkit.bronze.bronze_sales",
    comment="Raw sales transaction records — appended incrementally as new OBT files arrive",
    cluster_by=["order_id"],
)


@dp.append_flow(target="claude_aidevkit.bronze.bronze_sales")
def ingest_bronze_sales():
    return (
        _obt_stream()
        .select(
            F.col("Order_ID").alias("order_id"),
            F.col("Product_ID").alias("product_id"),
            F.col("Customer_ID").alias("customer_id"),
            F.col("Quantity").alias("quantity"),
            F.col("Discount").alias("discount"),
            F.col("Gross_Sales").alias("gross_sales"),
            F.col("Discount_Amount").alias("discount_amount"),
            F.col("Net_Sales").alias("net_sales"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        )
    )
