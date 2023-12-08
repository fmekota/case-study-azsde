# Keeping SQL template as a string in case of further dynamicall processing needed
SQL='''
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}` AS
(
SELECT 
    DISTINCT(trip_id),
    BIKE_ID, 
    (DURATION_MINUTES / 60.0) AS DURATION_HOURS, 
    START_STATION_NAME,
    DATE(start_time) AS TRIP_DATE
FROM 
    `{project_id}.{dataset_id}.bikeshare_trips`
WHERE 
    DATE(start_time) >= CAST("{start_date}" AS DATE) AND DATE(start_time) <= CAST("{end_date}" AS DATE)
    AND bike_type = 'electric'
    AND start_station_id = CAST(end_station_id AS INT64)
    AND start_station_id IN (
        SELECT station_id 
        FROM `{project_id}.{dataset_id}.bikeshare_stations`
        WHERE STATUS = 'active'
    )
ORDER BY trip_date
)
'''