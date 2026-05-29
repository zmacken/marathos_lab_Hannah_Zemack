from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_extract, when, size, split, get, lit
from utils.utils import rename_columns_to_snake_case


@dp.table(
    name="marathos.silver.marathos_obt",
    comment="Cleaned data for Marathos",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def cleaned_marathos():
    df = rename_columns_to_snake_case(
        spark.sql("SELECT * FROM STREAM marathos.bronze.raw_marathos")
    )

    df_countries = rename_columns_to_snake_case(
        spark.read.table("marathos.bronze.raw_country_code")
    ).select(
        col("iso3").alias("country_code"),
        col("country_common")
    )

    return (
        df
        .withColumn(
            "event_unit",
            regexp_extract(col("event_distance/length"), r"(km|mi|h)", 1)
        )
        .withColumn(
            "performance_clean",
            regexp_extract(col("athlete_performance"), r"^([\d:\.]+)", 1)
        )
        .withColumn(
            "performance_hours",
            when(
                col("event_unit").isin("km", "mi"),
                when(
                    size(split(col("performance_clean"), ":")) == 3,
                    get(split(col("performance_clean"), ":"), 0).cast("float") +
                    get(split(col("performance_clean"), ":"), 1).cast("float") / 60 +
                    get(split(col("performance_clean"), ":"), 2).cast("float") / 3600
                ).when(
                    size(split(col("performance_clean"), ":")) == 2,
                    get(split(col("performance_clean"), ":"), 0).cast("float") +
                    get(split(col("performance_clean"), ":"), 1).cast("float") / 60
                ).otherwise(lit(None))
            ).otherwise(lit(None))
        )
        .withColumn(
            "performance_km",
            when(
                col("event_unit") == "h",
                col("performance_clean").cast("float")
            ).otherwise(lit(None))
        )
        .join(
            df_countries,
            col("athlete_country") == col("country_code"),
            how="left"
        )
        .drop("country_code")
        .withColumnRenamed("country_common", "athlete_country_name")
    )