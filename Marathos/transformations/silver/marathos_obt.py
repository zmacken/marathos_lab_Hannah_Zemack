from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, regexp_extract, when, size, split, get, lit, regexp_replace,
    dense_rank, sha2, concat_ws, trim, expr
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

        #Klassificerar eventtyp baserat på distansformat så man inte missar de konstiga utstickarna
        .withColumn(
            "event_type",
            when(col("event_distance/length").rlike(r"Etappen"), "multi_stage")
            .when(col("event_distance/length").rlike(r"^\d+d$"), "time_based")
            .when(col("event_unit") == "h", "time_based")
            .when(col("event_unit").isin("km", "mi"), "distance_based")
            .otherwise("unknown")
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
                    size(split(col("performance_clean"), ":")) == 3,
                    expr("try_cast(split(performance_clean, ':')[0] AS FLOAT)") +
                    expr("try_cast(split(performance_clean, ':')[1] AS FLOAT)") / 60 +
                    expr("try_cast(split(performance_clean, ':')[2] AS FLOAT)") / 3600
                ).when(
                    size(split(col("performance_clean"), ":")) == 2,
                    expr("try_cast(split(performance_clean, ':')[0] AS FLOAT)") +
                    expr("try_cast(split(performance_clean, ':')[1] AS FLOAT)") / 60
                ).otherwise(lit(None))
            ).otherwise(lit(None))
        )

        # För tidbaserade lopp (h) används prestandavärdet direkt som distans i km
        .withColumn(
            "performance_km",
            when(
                col("event_unit") == "h",
                expr("try_cast(performance_clean AS FLOAT)") 
            ).otherwise(lit(None))
        )

        # Kopplar ihop med landkodstabellen för att slå upp atletens landsnamn
        # Behåller alla atleter även om landkoden saknas (left join)
        .join(
            df_countries,
            col("athlete_country") == col("country_code"),
            how="left"
        )
        .withColumnRenamed(
            "country_name", 
            "athlete_country_name"
        )
        .withColumnRenamed(
            "event_distance/length", 
            "event_distance_length"
        )

        # Extraherar landets förkortning från parentesen i slutet av eventnamnet, t.ex. "(GER)"
        .withColumn(
            "event_country", 
            regexp_extract(col("event_name"), r"\(([^)]+)\)$", 1)
        )

        # Tar bort landets förkortning och parentesen från eventnamnet för ett renare namn
        .withColumn(
            "event_name_clean", 
            regexp_replace(col("event_name"), r"\s*\([^)]+\)$", "")
        )

        #Fixa athelete_averafe_speed till kmh
        .withColumn(
            "athlete_avg_speed_kmh",
            col("athlete_average_speed").cast("double") / 1000
        )

        #skapa en athlete age för att senare lättare kunna göra beräkningar
        .withColumn(
            "athlete_age",
            when(
                col("athlete_year_of_birth").isNotNull(),
                col("year_of_event") - col("athlete_year_of_birth").cast("int")
            ).otherwise(lit(None))
        )

        #Normalisera tomma stränga i athlete_club till null
        .withColumn(
            "athlete_club",
            when(trim(col("athlete_club")) == "", lit(None)).otherwise(col("athlete_club"))
        )

        #Ta bort dagar som slutar på d
        .filter(
            ~col(
                "event_distance_length").rlike(r"^\d+d$"
            )
        )

        #Ta bort där event och perfomance är felaktigt
        .filter(
            ~(col("event_unit").isin("km", "mi") & col("performance_hours").isNull()) &
            ~((col("event_unit") == "h") & col("performance_km").isNull())  # ← parentes runt ==
        )

        # Tar bort kolumner som inte längre behövs efter transformationerna
        .drop(
            "athlete_performance", 
            "performance_clean", 
            "event_name", 
            "athlete_average_speed"
            )

        # --- Surrogatnycklar ---
        # event_id: deterministisk hash av det rensade eventnamnet.
        # sha2 används istället för dense_rank så att nyckeln är stabil mellan
        # pipeline-körningar och inte beror på radordning (säkert för streaming).
        .withColumn(
            "event_id",
            sha2(col("event_name_clean"), 256)
        )
        # athlete_id: finns redan som kolumn i källdata — ingen ny nyckel skapas.
    )