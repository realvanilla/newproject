def get_analytics_query(suffix):
    return f"""
-- Query for Final Table with 7-day data
WITH combined_events AS (
    -- Intraday events (most recent data)
    SELECT *, traffic_source.source AS session_source
    FROM `certain-catcher-417521.analytics_{suffix}.events_intraday_*`
    
    UNION ALL
    
    -- Historical events (prior days)
    SELECT *, traffic_source.source AS session_source
    FROM `certain-catcher-417521.analytics_{suffix}.events_*`
    WHERE _TABLE_SUFFIX BETWEEN 
        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) 
        AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)

SELECT
  sourceDataSet,
  landingPage,
  time_since_in_minutes as minutes_past,
  ifnull(sessionFirstCountry, "(not set)") as country,
  DATE(TIMESTAMP_MICROS(sessionFirstTimestamp)) as event_date,
  session_source,
  COUNT(DISTINCT session_uid) as sessions
FROM (
  --Prep Query for Final Table
  SELECT 
    sourceDataSet,
    event_timestamp,
    time_since_in_minutes,
    session_uid,
    eventName,
    landingPage,
    sessionFirstCountry,
    sessionFirstTimestamp,
    session_source
  FROM (
    SELECT 
      events.sourceDataset,
      events.session_uid,
      events.event_timestamp,
      events.eventName,
      sessionFirstCountry_tbl.sessionFirstCountry,
      sessionFirstTimestamp_tbl.sessionFirstTimestamp,
      TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP_MICROS(sessionFirstTimestamp_tbl.sessionFirstTimestamp),MINUTE) AS time_since_in_minutes,
      landingPage_tbl.landingPage,
      events.session_source
    FROM ( 
      SELECT 
        'certain-catcher-417521.analytics_{suffix}' AS sourceDataset,
        CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid,
        `event_timestamp` AS event_timestamp,
        event_name AS eventName,
        session_source
      FROM combined_events
    ) events
    INNER JOIN (
      SELECT session_uid,IFNULL(sessionFirstCountry,'(not set)') as sessionFirstCountry FROM (
      SELECT session_uid,(FIRST_VALUE(`country` IGNORE NULLS) OVER (PARTITION BY session_uid ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as sessionFirstCountry FROM ( 
      SELECT event_timestamp, CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid,
      geo.country as country FROM combined_events ) GROUP BY session_uid,country,event_timestamp
      ) group by session_uid,sessionFirstCountry
    ) sessionFirstCountry_tbl ON sessionFirstCountry_tbl.session_uid=events.session_uid
    INNER JOIN (
      SELECT session_uid,sessionFirstTimestamp FROM (
      SELECT session_uid,(FIRST_VALUE(`event_timestamp` IGNORE NULLS) OVER (PARTITION BY session_uid ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as sessionFirstTimestamp FROM ( 
      SELECT event_timestamp, CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid
      FROM combined_events ) GROUP BY session_uid,event_timestamp
      ) group by session_uid,sessionFirstTimestamp
    ) sessionFirstTimestamp_tbl ON sessionFirstTimestamp_tbl.session_uid=events.session_uid
    INNER JOIN (
      SELECT session_uid,landingPage FROM (
      SELECT CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid, 
      (SELECT params.value.string_value from UNNEST (event_params) as params where params.key='page_location') as landingPage, 
      event_name,
      row_number() over (PARTITION BY CONCAT(user_pseudo_id,(SELECT params.value.int_value from UNNEST (event_params) as params where params.key='ga_session_id')) ORDER BY event_timestamp) as sessionRank 
      FROM combined_events) 
      WHERE sessionRank=1) landingPage_tbl ON landingPage_tbl.session_uid=events.session_uid
    GROUP BY 
      sourceDataSet,
      session_uid,
      event_timestamp,
      sessionFirstTimestamp,
      time_since_in_minutes,
      eventName,
      sessionFirstCountry,
      landingPage,
      events.session_source)
  GROUP BY 
    sourceDataset,
    event_timestamp,
    time_since_in_minutes,
    session_uid,
    eventName,
    sessionFirstCountry,
    sessionFirstTimestamp,
    landingPage,
    session_source)
GROUP BY 
  sourceDataset,
  minutes_past,
  country,
  landingPage,
  event_date,
  session_source
    """

def get_historical_analytics_query(suffix):
    return f"""
WITH date_spine AS (
    SELECT date_day
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), 
                                   CURRENT_DATE())) as date_day
),
combined_events AS (
    SELECT *, traffic_source.source AS session_source
    FROM `certain-catcher-417521.analytics_{suffix}.events_*`
    WHERE _TABLE_SUFFIX BETWEEN 
        FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)) 
        AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),
historical_data AS (
    SELECT
      sourceDataSet,
      landingPage,
      1440 as minutes_past,
      ifnull(sessionFirstCountry, "(not set)") as country,
      DATE(TIMESTAMP_MICROS(sessionFirstTimestamp)) as event_date,
      session_source,
      COUNT(DISTINCT session_uid) as sessions
    FROM (
      SELECT 
        sourceDataSet,
        event_timestamp,
        1440 as time_since_in_minutes,
        session_uid,
        eventName,
        landingPage,
        sessionFirstCountry,
        sessionFirstTimestamp,
        session_source
      FROM (
        SELECT 
          events.sourceDataset,
          events.session_uid,
          events.event_timestamp,
          events.eventName,
          sessionFirstCountry_tbl.sessionFirstCountry,
          sessionFirstTimestamp_tbl.sessionFirstTimestamp,
          1440 AS time_since_in_minutes,
          landingPage_tbl.landingPage,
          events.session_source
        FROM ( 
          SELECT 
            'certain-catcher-417521.analytics_{suffix}' AS sourceDataset,
            CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid,
            `event_timestamp` AS event_timestamp,
            event_name AS eventName,
            session_source
          FROM combined_events
        ) events
        INNER JOIN (
          SELECT session_uid,IFNULL(sessionFirstCountry,'(not set)') as sessionFirstCountry FROM (
          SELECT session_uid,(FIRST_VALUE(`country` IGNORE NULLS) OVER (PARTITION BY session_uid ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as sessionFirstCountry FROM ( 
          SELECT event_timestamp, CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid,
          geo.country as country FROM combined_events ) GROUP BY session_uid,country,event_timestamp
          ) group by session_uid,sessionFirstCountry
        ) sessionFirstCountry_tbl ON sessionFirstCountry_tbl.session_uid=events.session_uid
        INNER JOIN (
          SELECT session_uid,sessionFirstTimestamp FROM (
          SELECT session_uid,(FIRST_VALUE(`event_timestamp` IGNORE NULLS) OVER (PARTITION BY session_uid ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) as sessionFirstTimestamp FROM ( 
          SELECT event_timestamp, CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid
          FROM combined_events ) GROUP BY session_uid,event_timestamp
          ) group by session_uid,sessionFirstTimestamp
        ) sessionFirstTimestamp_tbl ON sessionFirstTimestamp_tbl.session_uid=events.session_uid
        INNER JOIN (
          SELECT session_uid,landingPage FROM (
          SELECT CONCAT((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),'_',user_pseudo_id) AS session_uid, 
          (SELECT params.value.string_value from UNNEST (event_params) as params where params.key='page_location') as landingPage, 
          event_name,
          row_number() over (PARTITION BY CONCAT(user_pseudo_id,(SELECT params.value.int_value from UNNEST (event_params) as params where params.key='ga_session_id')) ORDER BY event_timestamp) as sessionRank 
          FROM combined_events) 
          WHERE sessionRank=1) landingPage_tbl ON landingPage_tbl.session_uid=events.session_uid
        GROUP BY 
          sourceDataSet,
          session_uid,
          event_timestamp,
          sessionFirstTimestamp,
          time_since_in_minutes,
          eventName,
          sessionFirstCountry,
          landingPage,
          events.session_source)
      GROUP BY 
        sourceDataset,
        event_timestamp,
        time_since_in_minutes,
        session_uid,
        eventName,
        sessionFirstCountry,
        sessionFirstTimestamp,
        landingPage,
        session_source)
    GROUP BY 
      sourceDataset,
      minutes_past,
      country,
      landingPage,
      event_date,
      session_source
),
date_based_data AS (
    SELECT
      date_spine.date_day as event_date,
      COALESCE(historical_data.sourceDataSet, 'certain-catcher-417521.analytics_{suffix}') as sourceDataSet,
      COALESCE(historical_data.landingPage, '') as landingPage,
      COALESCE(historical_data.minutes_past, 1440) as minutes_past,
      COALESCE(historical_data.country, 'United States') as country,
      COALESCE(historical_data.session_source, '') as session_source,
      COALESCE(historical_data.sessions, 0) as sessions
    FROM date_spine
    LEFT JOIN historical_data ON date_spine.date_day = historical_data.event_date
)

SELECT * FROM date_based_data

UNION ALL

SELECT
  CURRENT_DATE() as event_date,
  'certain-catcher-417521.analytics_{suffix}' as sourceDataSet,
  '' as landingPage,
  30 as minutes_past,
  '' as country,
  '' as session_source,
  0 as sessions
"""