@echo off

REM Prompt the user for start and end dates
set /p start_date="Enter start date (DD.MM.YYYY): "
set /p end_date="Enter end date (DD.MM.YYYY): "

REM Create a temporary copy of env.yaml
copy C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\blob_storage\env.yaml C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\blob_storage\temp_env.yaml
copy C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\devizovy_trh\env.yaml C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\devizovy_trh\temp_env.yaml
copy C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\weather_report\env.yaml C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\weather_report\temp_env.yaml
copy C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\public_trips\env.yaml C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\public_trips\temp_env.yaml

REM Add the variables to the temporary env.yaml files
for /r ./CloudFunctions/blob_storage %%f in (temp_env.yaml) do (
    echo.>>"%%f"
    echo START_DATE: "%start_date%">>"%%f"
    echo END_DATE: "%end_date%">>"%%f"
)

for /r ./CloudFunctions/devizovy_trh %%f in (temp_env.yaml) do (
    echo.>>"%%f"
    echo START_DATE: "%start_date%">>"%%f"
    echo END_DATE: "%end_date%">>"%%f"
)

for /r ./CloudFunctions/weather_report %%f in (temp_env.yaml) do (
    echo.>>"%%f"
    echo START_DATE: "%start_date%">>"%%f"
    echo END_DATE: "%end_date%">>"%%f"
)

for /r ./CloudFunctions/public_trips %%f in (temp_env.yaml) do (
    echo.>>"%%f"
    echo START_DATE: "%start_date%">>"%%f"
    echo END_DATE: "%end_date%">>"%%f"
)

echo "Deploying function: blob_storage"
 
call gcloud functions deploy blob_storage ^
    --runtime python310 ^
    --trigger-http ^
    --allow-unauthenticated ^
    --source ./CloudFunctions/blob_storage/ ^
    --env-vars-file CloudFunctions/blob_storage/temp_env.yaml ^
    --entry-point download_and_upload_to_bq ^
    --gen2

echo "Function blob_storage deployed successfully"

echo "Deploying function: devizovy_trh"
call gcloud functions deploy devizovy_trh ^
    --runtime python310 ^
    --trigger-http ^
    --allow-unauthenticated ^
    --source ./CloudFunctions/devizovy_trh/ ^
    --env-vars-file CloudFunctions/devizovy_trh/temp_env.yaml ^
    --entry-point get_rate_data_and_upload_to_bq ^
    --gen2

echo "Function devizovy_trh deployed successfully"

echo "Deploying function: weather_report"
call gcloud functions deploy weather_report ^
    --runtime python310 ^
    --trigger-http ^
    --allow-unauthenticated ^
    --source ./CloudFunctions/weather_report/ ^
    --env-vars-file CloudFunctions/weather_report/temp_env.yaml ^
    --entry-point download_and_store_weather_report ^
    --gen2

echo "Function weather_report deployed successfully"

echo "Deploying function: public_trips"
call gcloud functions deploy public_trips ^
    --runtime python310 ^
    --trigger-http ^
    --allow-unauthenticated ^
    --source ./CloudFunctions/public_trips/ ^
    --env-vars-file CloudFunctions/public_trips/temp_env.yaml ^
    --entry-point get_trips_from_public_dataset ^
    --gen2

echo "Function public_trips deployed successfully"

echo "Deploying workflow"
call gcloud workflows deploy workflow ^
    --source ./Workflow/case_study_workflow.yaml ^
    --location europe-west3

echo "Workflow deployed successfully"

REM Clean up temporary env.yaml files
del C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\blob_storage\temp_env.yaml
del C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\devizovy_trh\temp_env.yaml
del C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\weather_report\temp_env.yaml
del C:\Users\Filip\Documents\case-study-azsde\case-study-azsde\CloudFunctions\public_trips\temp_env.yaml