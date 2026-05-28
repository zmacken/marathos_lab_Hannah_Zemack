CREATE OR REFRESH STREAMING TABLE marathos.bronze.raw_country_code
  COMMENT "Raw country code - bronze layer" AS
SELECT
  *
FROM
  STREAM read_files(
    "/Volumes/marathos/default/raw/data_country",
    format => "csv",
    header => "true",
    inferSchema => "true"
  )