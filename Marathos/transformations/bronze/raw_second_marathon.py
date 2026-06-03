from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

BASE_DIR = "/Volumes/marathos/default/raw"

schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_DIR}/data_second_marathon")
    .schema
)

@dp.table(
    name="marathos.bronze.raw_second_marathon",
    comment="Raw second marathon data as the bronze layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def raw_second_marathon():
    return (
        spark.readStream.format("csv")
        .options(header="true", inferSchema="true", encoding="UTF-8")
        .schema(schema)
        .load(f"{BASE_DIR}/data_second_marathon")
    )