# case-study-azsde
This codebase is a PoC for the case study assigned on 2023-12-01. Goal is to design and implement simple, yet functional pipeline within the context of GCP 

Initial setup:
Manually created scheduler, targeting the "blob_storage" cloud function
Manually created Pub/Sub topic, to pass messages from "blob_storage" cloud function to "devizovy_trh" and "weather_report" cloud functions

Functions deployed by deploy.bat script from root folder

'''
deploy.bat
start_date = [USER_INPUT]
end_date = [USER_INPUT]
'''
Prior deployment, keep in mind that each of the functions has separate env-file to keep the microservice pattern



