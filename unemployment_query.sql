-- i used the scheduling bigquery options. the only important point of 
-- this query is this line, which enables to request 1 week ago
--     AND DATE(_PARTITIONTIME) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))  


SELECT
  -- este select está solo para usar el where al final y eliminar los null que se generan en news_in_Spain
  *
FROM (
  SELECT
    EXTRACT (date
    FROM
      PARSE_TIMESTAMP('%Y%m%d%H%M%S',CAST(date AS string))) AS Date,
    SourceCommonName,
    DocumentIdentifier,
    ROUND(CAST(SPLIT(V2Tone, ",") [
      OFFSET
        (0)] AS FLOAT64),2) AS Sentiment,
    (CASE

        WHEN V2Themes LIKE "%UNEMPLOYMENT%" THEN "tagged by Gdelt as unemployment-related"
        -- WHEN LOWER(DocumentIdentifier) LIKE "%desempleo%" THEN "unemployment explicitly found in the article" 
    END
      ) AS news_in_Spain,

  FROM
    `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE
    v2counts LIKE '%#SP#%'
    AND counts LIKE '%#SP#%'
    AND V2Locations LIKE '%#SP#%'
    AND DATE(_PARTITIONTIME) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))  
  
    AND ( SourceCommonName IN (
      SELECT
        spanish_newspapers
      FROM
      -- I created my own media tables to filter by country newspapers
        `project-test-3105.gdelt_info_filtering.spanish_newspapers_SourceCommonName_160620`)))
        
WHERE
  news_in_Spain IS NOT NULL
  
  
