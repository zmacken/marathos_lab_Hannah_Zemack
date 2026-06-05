# Marathos Data Engineering Platform

Detta projekt är en end-to-end dataplattform och ETL-pipeline byggd i Databricks för en skollabb

## Live Dashboard & Projektlänkar
* **Interaktiv Dashboard:** [Klicka här för att öppna Databricks Dashboard](https://dbc-e028f456-6853.cloud.databricks.com/dashboardsv3/01f15f1f8bbb1765b23fc7b22cdd70bd/published?o=7474646102100893&f_global_filters%7Eyear_filter=_all_)

## Tech Stack
* **Plattform:** Databricks (Unity Catalog)
* **Språk & Bibliotek:** Python, PySpark, SQL, Plotly
* **Versionshantering:** Git & GitHub

## Arkitektur & Implementation
Projektet är uppbyggt enligt **Medallion-arkitekturen** i Unity Catalog och följer strikt **DRY-principen** (Don't Repeat Yourself) för ren och återanvändbar kod:

* **Bronze:** Ingestering och strukturering av rådata i Unity Catalog.
* **Silver:** Datatvätt, hantering av ogiltiga rader, tids- och enhetskonverteringar samt generering av unika ID:n (OBT-struktur).
* **Gold:** Dimensionell modellering (stjärnschema) med faktatabeller (`fct_results`) och dimensionstabeller (`dim_event`, `dim_athlete`) samt optimerade vyer.

##  Avancerad Funktionalitet (Bonus)
Samtliga bonusmål i databricks_lab.pdf har implementerats i projektet:
* **Dataströmmar (Streaming):** Simulering och strömning av ny maratondata genererad via LLM direkt in i Unity Catalog.
* **Automation:** Hela datapipelinen är schemalagd och automatiserad via Databricks Workflows/Jobs.
* **Databerikning:** Ingestering av landskoder via externa källor/LLM för djupare geografisk analys.
* **Datumdimension:** Skapat en dedikerad och optimerad *Date Dimension*.
* **Marathos Genie:** Driftsatt en AI-assistent för ad-hoc SQL-frågor, där svarens korrekthet har verifierats manuellt mot notebooks.

## Repostruktur
Källkoden är strukturerad enligt följande:
* `explorations/` - Explorativ dataanalys (EDA)[cite: 1].
* `transformations/` - Folders för pipeline-stegen (Bronze, Silver, Gold).
* `utils/` - Återanvändbara hjälpfunktioner och moduler.



LLM has been used for troubleshooting but not for whole code blocks.
