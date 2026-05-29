from pyspark import pipelines as dp
from pyspark.sql.functions import col, regexp_extract, when, size, split, get, lit, regexp_replace
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
    # Läser in rådata som en ström från bronze-lagret och normaliserar kolumnnamnen till snake_case
    df = rename_columns_to_snake_case(
        spark.sql("SELECT * FROM STREAM marathos.bronze.raw_marathos")
    )

    # Läser in landkodstabellen och väljer ut ISO3-koden samt det vanliga landsnamnet
    df_countries = rename_columns_to_snake_case(
        spark.read.table("marathos.bronze.raw_country_code")
    ).select(
        col("iso3").alias("country_code"),
        col("country_common")
    )

    return (
        df
        # Extraherar enheten från event_distance/length-kolumnen (t.ex. "km", "mi" eller "h")
        .withColumn(
            "event_unit",
            regexp_extract(col("event_distance/length"), r"(km|mi|h)", 1)
        )
        # Extraherar den numeriska delen av prestandavärdet (t.ex. "2:30:45" ur "2:30:45 h")
        .withColumn(
            "performance_clean",
            regexp_extract(col("athlete_performance"), r"^([\d:\.]+)", 1)
        )
        # Omvandlar prestationen till timmar för distanslopp (km/mi)
        # Hanterar både HH:MM:SS- och MM:SS-format
        .withColumn(
            "performance_hours",
            when(
                col("event_unit").isin("km", "mi"),
                when(
                    # HH:MM:SS-format → timmar + minuter/60 + sekunder/3600
                    size(split(col("performance_clean"), ":")) == 3,
                    get(split(col("performance_clean"), ":"), 0).cast("float") +
                    get(split(col("performance_clean"), ":"), 1).cast("float") / 60 +
                    get(split(col("performance_clean"), ":"), 2).cast("float") / 3600
                ).when(
                    # MM:SS-format → minuter + sekunder/60
                    size(split(col("performance_clean"), ":")) == 2,
                    get(split(col("performance_clean"), ":"), 0).cast("float") +
                    get(split(col("performance_clean"), ":"), 1).cast("float") / 60
                ).otherwise(lit(None))
            ).otherwise(lit(None))
        )
        # För tidbaserade lopp (h) används prestandavärdet direkt som distans i km
        .withColumn(
            "performance_km",
            when(
                col("event_unit") == "h",
                col("performance_clean").cast("float")
            ).otherwise(lit(None))
        )
        # Kopplar ihop med landkodstabellen för att slå upp atletens landsnamn
        .join(
            df_countries,
            col("athlete_country") == col("country_code"),
            how="left"  # Behåller alla atleter även om landkoden saknas
        )
        .withColumnRenamed("country_common", "athlete_country_name")
        # Ta ut namnet utan country code
        .withColumn("event_country", regexp_extract(col("event_name"), r"\(([^)]+)\)$", 1))
        # Ta bort landet + parentes från event-namnet
        .withColumn("event_name_clean", regexp_replace(col("event_name"), r"\s*\([^)]+\)$", ""))
        # Tar bort hjälpkolumnen country_code och döper om landsnamnet till ett tydligare namn
        .drop("athlete_performance", "performance_clean", "event_name")
        
    )