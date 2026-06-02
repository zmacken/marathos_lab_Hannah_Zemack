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

    # ---------------------------
    # Läs in rådata (bronze)
    # ---------------------------
    df = rename_columns_to_snake_case(
        spark.sql("SELECT * FROM STREAM marathos.bronze.raw_marathos")
    )

    # ---------------------------
    # Läs in landkodstabell
    # ---------------------------
    df_countries = rename_columns_to_snake_case(
        spark.read.table("marathos.bronze.raw_country_code")
    )

    # ---------------------------
    # skapa separata lookup-tabeller för att undvika ambiguitet
    # (lösning på AMBIGUOUS_REFERENCE country_code)
    # ---------------------------
    countries_athlete = df_countries.select(
        col("country_code").alias("athlete_country_code"),
        col("country_name").alias("athlete_country_name")
    )

    countries_event = df_countries.select(
        col("country_code").alias("event_country_code"),
        col("country_name").alias("event_country_name")
    )

    return (
        df
        # Extrahera enhet (km, mi, h)
        .withColumn(
            "event_unit",
            regexp_extract(col("event_distance/length"), r"(km|mi|h)", 1)
        )

        # Klassificera eventtyp
        .withColumn(
            "event_type",
            when(col("event_distance/length").rlike(r"Etappen"), "multi_stage")
            .when(col("event_distance/length").rlike(r"^\d+d$"), "time_based")
            .when(col("event_unit") == "h", "time_based")
            .when(col("event_unit").isin("km", "mi"), "distance_based")
            .otherwise("unknown")
        )

        # Rensa prestation
        .withColumn(
            "performance_clean",
            regexp_extract(col("athlete_performance"), r"^([\d:\.]+)", 1)
        )

        # Omvandla tid till timmar
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

        # För tidsbaserade lopp (h)
        .withColumn(
            "performance_km",
            when(
                col("event_unit") == "h",
                expr("try_cast(performance_clean AS FLOAT)")
            ).otherwise(lit(None))
        )

        # ---------------------------
        # Första joinen (tydlig aliasad lookup)
        # ---------------------------
        .join(
            countries_athlete,
            col("athlete_country") == col("athlete_country_code"),
            "left"
        )

        # ---------------------------
        # Byt namn på kolumn efter join
        # ---------------------------
        .withColumnRenamed(
            "event_distance/length",
            "event_distance_length"
        )


        # ta bort landeskoden ur event
        .withColumn(
            "event_country",
            regexp_extract(col("event_name"), r"\(([^)]+)\)$", 1)
        )

        # ---------------------------
        # andra joinen (separat lookup → ingen ambiguity)
        # ---------------------------
        .join(
            countries_event,
            col("event_country_code") == col("event_country_code"),
            "left"
        )

        # Rensa eventnamn
        .withColumn(
            "event_name_clean",
            regexp_replace(col("event_name"), r"\s*\([^)]+\)$", "")
        )

        # Snitthastighet
        .withColumn(
            "athlete_avg_speed_kmh",
            expr("try_cast(athlete_average_speed as double)")
        )

        # Ålder
        .withColumn(
            "athlete_age",
            when(
                col("athlete_year_of_birth").isNotNull(),
                col("year_of_event") - col("athlete_year_of_birth").cast("int")
            ).otherwise(lit(None))
        )

        # Rensa klubb
        .withColumn(
            "athlete_club",
            when(trim(col("athlete_club")) == "", lit(None)).otherwise(col("athlete_club"))
        )

        # Datumhantering
        .withColumn(
            "event_start_date",
            when(
                col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}$"),
                expr("try_to_date(event_dates, 'dd.MM.yyyy')")
            ).when(
                col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}$"),
                expr("""
                    try_to_date(
                        concat(
                            regexp_extract(event_dates, '^(\\d{2})\\.', 1),
                            '.',
                            regexp_extract(event_dates, '^\\d{2}\\.-\\d{2}\\.(\\d{2}\\.\\d{4})$', 1)
                        ),
                        'dd.MM.yyyy'
                    )
                """)
            ).otherwise(lit(None))
        )

        # Filtrera bort etapper
        .filter(~col("event_distance_length").rlike(r"^\d+d$"))

        # Filtrera felaktiga rader
        .filter(
            ~(col("event_unit").isin("km", "mi") & col("performance_hours").isNull()) &
            ~((col("event_unit") == "h") & col("performance_km").isNull())
        )

        # Ta bort onödiga kolumner
        .drop(
            "athlete_performance",
            "performance_clean",
            "event_name",
            "athlete_average_speed"
        )

        # Surrogatnyckel
        .withColumn(
            "event_id",
            sha2(col("event_name_clean"), 256)
        )
    )