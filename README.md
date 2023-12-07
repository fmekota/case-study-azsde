# case-study-azsde
This codebase is a PoC for the case study assigned on 2023-12-01. Goal is to design and implement simple, yet functional pipeline within the context of GCP 

Initial setup:
Manually created workflow environment to setup a scheduler to run each hour
Assigned Service account needs Cloud Function Invoker IAM role

Functions deployed by deploy.bat script from root folder

<code>deploy.bat
 start_date = [USER_INPUT]
 end_date = [USER_INPUT]
</code>

Prior deployment, keep in mind that each of the functions has separate env-file to keep the microservice pattern



