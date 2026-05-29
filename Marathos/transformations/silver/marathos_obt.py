from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, regexp_extract, when, size, split, get, lit, regexp_replace,
    dense_rank, sha2, concat_ws
)
from pyspark.sql.window import Window
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
                    # MM:SS-format → minuter/60 + sekunder/3600
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
        # Behåller alla atleter även om landkoden saknas (left join)
        .join(
            df_countries,
            col("athlete_country") == col("country_code"),
            how="left"
        )
        .withColumnRenamed("country_name", "athlete_country_name")
        .withColumnRenamed("event_distance/length", "event_distance_length")
        # Extraherar landets förkortning från parentesen i slutet av eventnamnet, t.ex. "(GER)"
        .withColumn("event_country", regexp_extract(col("event_name"), r"\(([^)]+)\)$", 1))
        # Tar bort landets förkortning och parentesen från eventnamnet för ett renare namn
        .withColumn("event_name_clean", regexp_replace(col("event_name"), r"\s*\([^)]+\)$", ""))
        # Tar bort kolumner som inte längre behövs efter transformationerna
        .drop("athlete_performance", "performance_clean", "event_name")

        # --- Surrogatnycklar ---
        # event_id: deterministisk hash av det rensade eventnamnet.
        # sha2 används istället för dense_rank så att nyckeln är stabil mellan
        # pipeline-körningar och inte beror på radordning (säkert för streaming).
        .withColumn(
            "event_id",
            sha2(col("event_name_clean"), 256)
        )
        # athlete_id: finns redan som kolumn i källdata — ingen ny nyckel skapas.
        # Kolumnen behålls som den är från bronze-lagret.
    )