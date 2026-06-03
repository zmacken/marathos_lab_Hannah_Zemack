from pyspark import pipelines as dp  # Importerar Databricks pipeline-bibliotek för att definiera tabeller
from pyspark.sql.functions import (
    col, regexp_extract, when, size, split, get, lit, regexp_replace,  # Importerar vanliga kolumnfunktioner
    dense_rank, sha2, concat_ws, trim, expr, to_date, concat           # Importerar fler funktioner för ranking, hashing, datum m.m.
)
from pyspark.sql.window import Window  # Importerar Window för fönsterfunktioner (t.ex. ranking per grupp)
from utils.utils import rename_columns_to_snake_case  # Importerar hjälpfunktion som döper om kolumner till snake_case

@dp.table(
    name="marathos.silver.marathos_obt",  # Definierar måltabellens fullständiga namn (katalog.schema.tabell)
    comment="Cleaned data for Marathos",  # Lägger till en beskrivande kommentar på tabellen
    table_properties={
        "delta.columnMapping.mode": "name",   # Aktiverar kolumnmappning via namn (stöder specialtecken i kolumnnamn)
        "delta.minReaderVersion": "2",         # Kräver minst Delta reader version 2
        "delta.minWriterVersion": "5"          # Kräver minst Delta writer version 5
    }
)
def cleaned_marathos():

    # ---------------------------
    # Läs in rådata (bronze)
    df_main = rename_columns_to_snake_case(
        spark.sql("SELECT * FROM STREAM (marathos.bronze.raw_marathos)")  # Läser in huvuddatan som en ström från bronzetabellen
    )

    df_second = rename_columns_to_snake_case(
        spark.sql("SELECT * FROM STREAM(marathos.bronze.raw_second_marathon)")  # Läser in andra maratonkällan som ström (wrappas i STREAM för att fungera med union)
    )

    df = df_main.unionByName(df_second, allowMissingColumns=True)  # Slår ihop båda dataframes radvis på kolumnnamn; saknade kolumner fylls med null

    # ---------------------------
    # Läs in landkodstabell
    # ---------------------------
    df_countries = rename_columns_to_snake_case(
        spark.read.table("marathos.bronze.raw_country_code")  # Läser in landkodstabellen som en statisk batch (inte ström)
    )

    # ---------------------------
    # Skapa separata lookup-tabeller för att undvika ambiguitet
    # (lösning på AMBIGUOUS_REFERENCE country_code)
    # ---------------------------
    countries_athlete = df_countries.select(
        col("country_code").alias("athlete_country_code"),  # Byter namn på country_code för att användas vid join mot atlet
        col("country_name").alias("athlete_country_name")   # Byter namn på country_name för atletens land
    )

    countries_event = df_countries.select(
        col("country_code").alias("event_country_code"),  # Byter namn på country_code för att användas vid join mot event
        col("country_name").alias("event_country_name")   # Byter namn på country_name för eventets land
    )

    return (
        df
        # Extrahera enhet (km, mi, h)
        .withColumn(
            "event_unit",
            regexp_extract(col("event_distance/length"), r"(km|mi|h)", 1)  # Plockar ut enheten (km, mi eller h) ur distanskolumnen med regex
        )

        # Klassificera eventtyp
        .withColumn(
            "event_type",
            when(col("event_distance/length").rlike(r"Etappen"), "multi_stage")       # Om distansen innehåller "Etappen" → flerstegslopp
            .when(col("event_distance/length").rlike(r"^\d+d$"), "time_based")        # Om formatet är t.ex. "6d" (dagar) → tidsbaserat
            .when(col("event_unit") == "h", "time_based")                             # Om enheten är timmar → tidsbaserat
            .when(col("event_unit").isin("km", "mi"), "distance_based")               # Om enheten är km eller mi → distansbaserat
            .otherwise("unknown")                                                      # Allt annat klassas som okänt
        )

        # Extrahera numeriskt avstånd/tid
        .withColumn(
            "event_distance_numeric",
            regexp_extract(col("event_distance/length"), r"^([\d.]+)", 1).cast("float")  # Plockar ut det numeriska värdet från distansen och castar till float
        )

        # Rensa prestation
        .withColumn(
            "performance_clean",
            regexp_extract(col("athlete_performance"), r"^([\d:\.]+)", 1)  # Extraherar endast tid/sifferdelen ur prestationskolumnen (tar bort eventuell skräptext)
        )

        # Omvandla tid till timmar
        .withColumn(
            "performance_hours",
            when(
                col("event_unit").isin("km", "mi"),          # Gäller bara distansbaserade lopp
                when(
                    size(split(col("performance_clean"), ":")) == 3,  # Om formatet är HH:MM:SS
                    expr("try_cast(split(performance_clean, ':')[0] AS FLOAT)") +          # Hämtar timmar
                    expr("try_cast(split(performance_clean, ':')[1] AS FLOAT)") / 60 +    # Omvandlar minuter till timmar
                    expr("try_cast(split(performance_clean, ':')[2] AS FLOAT)") / 3600    # Omvandlar sekunder till timmar
                ).when(
                    size(split(col("performance_clean"), ":")) == 2,  # Om formatet är MM:SS
                    expr("try_cast(split(performance_clean, ':')[0] AS FLOAT)") +          # Hämtar minuter
                    expr("try_cast(split(performance_clean, ':')[1] AS FLOAT)") / 60      # Omvandlar sekunder till minuter
                ).otherwise(lit(None))  # Om inget format matchar → null
            ).otherwise(lit(None))      # Om inte distansbaserat → null
        )

        # För tidsbaserade lopp (h)
        .withColumn(
            "performance_km",
            when(
                col("event_unit") == "h",                          # Gäller bara tidsbaserade lopp (enheten är timmar)
                expr("try_cast(performance_clean AS FLOAT)")       # Castar prestationen direkt till float (km-värde)
            ).otherwise(lit(None))                                 # Annars null
        )

        # Första joinen (tydlig aliasad lookup)
        .join(
            countries_athlete,
            col("athlete_country") == col("athlete_country_code"),  # Kopplar atletens landkolumn mot landkodslookup
            "left"                                                   # Left join: behåller alla rader även om inget land matchas
        )

        # Byt namn på kolumn efter join
        .withColumnRenamed(
            "event_distance/length",   # Ursprungligt kolumnnamn (med specialtecken)
            "event_distance_length"    # Nytt rensat kolumnnamn utan snedstreck
        )

        # Ta bort landskoden ur event
        .withColumn(
            "event_country",
            regexp_extract(col("event_name"), r"\(([^)]+)\)$", 1)  # Plockar ut landkoden inom parentes i slutet av eventnamnet, t.ex. "(SWE)"
        )

        # Andra joinen (separat lookup → ingen ambiguity)
        .join(
            countries_event,
            col("event_country") == col("event_country_code"),  # Kopplar eventets extraherade landkod mot landkodslookup
            "left"                                               # Left join: behåller alla rader även utan matchning
        )

        # Rensa eventnamn
        .withColumn(
            "event_name_clean",
            regexp_replace(col("event_name"), r"\s*\([^)]+\)$", "")  # Tar bort landkodsdelen "(SWE)" från slutet av eventnamnet
        )

        # Snitthastighet
        .withColumn(
            "athlete_avg_speed_kmh",
            expr("try_cast(regexp_replace(athlete_average_speed, ',', '.') as double)")  # Byter komma mot punkt och castar till double för att hantera europeiskt decimalformat
        )

        # Ålder
        .withColumn(
            "athlete_age",
            when(
                col("athlete_year_of_birth").isNotNull(),                              # Räknar bara ålder om födelseår finns
                col("year_of_event") - col("athlete_year_of_birth").cast("int")        # Beräknar ålder som eventår minus födelseår
            ).otherwise(lit(None))                                                     # Annars null
        )

        # Rensa klubb
        .withColumn(
            "athlete_club",
            when(trim(col("athlete_club")) == "", lit(None)).otherwise(col("athlete_club"))  # Ersätter tomma strängar med null i klubbkolumnen
        )

        # Datumhantering
        .withColumn(
            "event_start_date",
            # Enkelt datum: 1991-05-01
            when(
                col("event_dates").rlike(r"^\d{4}-\d{2}-\d{2}$"),         # Matchar ISO-format YYYY-MM-DD
                to_date(col("event_dates"), "yyyy-MM-dd")                   # Parsar till datumtyp
            )
            # Enkelt datum: 01.05.1991
            .when(   
                col("event_dates").rlike(r"^\d{2}\.\d{2}\.\d{4}$"),        # Matchar europeiskt format DD.MM.YYYY
                to_date(col("event_dates"), "dd.MM.yyyy")                   # Parsar till datumtyp
            )
            # Spann variant 1: 23.-24.11.1991
            .when(
                col("event_dates").rlike(r"^\d{2}\.-\d{2}\.\d{2}\.\d{4}$"),  # Matchar datumspann där bara dag anges för startdatum
                to_date(
                    concat(
                        regexp_extract(col("event_dates"), r"^(\d{2})", 1),        # Plockar ut startdagen
                        lit("."),
                        regexp_extract(col("event_dates"), r"\.(\d{2}\.\d{4})$", 1)  # Plockar ut månad och år från slutet
                    ),
                    "dd.MM.yyyy"  # Sätter ihop och parsar till datum
                )
            )
            # Spann variant 2: 30.04.-01.05.2016
            .when(
                col("event_dates").rlike(r"^\d{2}\.\d{2}\.-\d{2}\.\d{2}\.\d{4}$"),  # Matchar fullt datumspann DD.MM.-DD.MM.YYYY
                to_date(
                    concat(
                        regexp_extract(col("event_dates"), r"^(\d{2}\.\d{2}\.)", 1),  # Plockar ut startdatum (dag och månad)
                        regexp_extract(col("event_dates"), r"(\d{4})$", 1)             # Plockar ut år från slutet
                    ),
                    "dd.MM.yyyy"  # Sätter ihop och parsar till datum
                )
            )
            .otherwise(lit(None))  # Om inget datumformat matchar → null
        )

        # Filtrera bort etapper
        .filter(~col("event_distance_length").rlike(r"^\d+d$"))  # Tar bort rader där distansen är flerstagsformat (t.ex. "6d")

        # Filtrera felaktiga rader
        .filter(
            ~(col("event_unit").isin("km", "mi") & col("performance_hours").isNull()) &  # Tar bort distanslopp utan beräknad tid
            ~((col("event_unit") == "h") & col("performance_km").isNull())               # Tar bort tidslopp utan beräknad km-prestation
        )

        # Ta bort onödiga kolumner
        .drop(
            "athlete_performance",      # Råprestationskolumnen (ersatt av performance_hours/performance_km)
            "performance_clean",        # Mellanliggande rensad prestation (inte längre behövd)
            "event_name",               # Råeventnamnet (ersatt av event_name_clean)
            "athlete_average_speed",    # Råhastighetkolumn (ersatt av athlete_avg_speed_kmh)
            "event_dates",              # Rådatumkolumn (ersatt av event_start_date)
            "event_distance_length"     # Rådistanskolumn (ersatt av event_distance_numeric och event_unit)
        )

        # Surrogatnyckel
        .withColumn(
            "event_id",
            sha2(col("event_name_clean"), 256)  # Skapar en unik hash-nyckel för varje event baserat på eventnamnet (SHA-256)
        )
    )