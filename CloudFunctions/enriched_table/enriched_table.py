# Keeping SQL template as a string in case of further dynamicall processing needed
SQL='''
CREATE OR REPLACE TABLE `{project_id}.{oz_dataset_id}.{table_id}` AS (
                    SELECT 
                    bt.trip_id,
                    bt.bike_id,
                    bt.duration_hours,
                    bt.start_station_name,
                    bt.trip_date,
                    wd.temp,
                    bd.manufacturer,
                    (bd.price_eur * dd.Rate) AS bike_price
                    FROM `{project_id}.{lz_dataset_id}.bike_trips` AS bt
                    JOIN `{project_id}.{lz_dataset_id}.weather_data` AS wd
                    ON bt.TRIP_DATE = DATE(wd.datetime)
                    JOIN `{project_id}.{lz_dataset_id}.bikes_data` AS bd
                    ON bt.BIKE_ID = CAST(bd.bike_id AS STRING)
                    JOIN `{project_id}.{lz_dataset_id}.devizova_data` AS dd
                    ON bt.TRIP_DATE = dd.Datum
                    )
'''