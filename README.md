https://github.com/ferrarisf50/Sparkify_Redshift/blob/master/create_cluster.py


# sparkify-redshift-data-warehouse

# Creating a Redshift Data Warehouse on AWS for the Music Streaming Service Sparkify

## Table of Contents
1. [Project Motivation and Description](#Project-Motivation)
2. [Installation](#Installation)
3. [File Descriptions](#File-Descriptions)
4. [Authors and Acknowledgements](#Authors-Acknowledgements)

## Project Motivation and Description <a name="Project-Motivation"></a>

The analytics team of the fictional music streaming service Sparkify wants get enabled to understand what songs users are listening to. 
This project aims to support this need by modeling log data which resides in S3 in JSON format and setting up a redshift data warehouse in the Amazon cloud to make the data available for analysis.
In more detail, a cloud based ETL pipeline has to be implemented to load data from S3, transform it and load it into the newly designed and created redshift database.

## Installation <a name="Installation"></a>

In AWS: Create IAM user (dwhadmin)

1. Add AWS KEY and SECRET into dwh_example.cfg and save it under dwh.cfg
2. Run create_cluster.py
3. Add DWH_ENDPOINT and DWH_ROLE_ARN into dwh.cfg (they get logged in step 2)
4. Run create_tables.py
5. Run etl.py


**Preparation:**
First step: create IAM User in AWS
- create IAM user (dwhadmin)
- give privileges to user
- use access token and secret to buld cluster and configure it

Add info to confi
Create dwh.cfg and add the access token and secret for the newly created user provided by AWS


(use the script create_cluster.py or do it manuellyy)
Add DWH_ENDPOINT and DWH_ROLE_ARN in dwh.cfg
Programatically create aws redshift cluster
Install boto3 (python AWS SDK) - conda install boto3 or pip install boto3


**Run Scripts:**
* Run create_tables.py to create the tables in AWS Redshift
* Run etl.py to load data from staging tables to analytics tables on Redshift

**Make sure to delete your redshift cluster afterwards if not needed anymore to prevent unnecessary costs.**

## File Descriptions <a name="File-Descriptions"></a>



* create_tables.py: This script creates the fact and dimension tables defined in sql_queries.py in Redshift
* etl.py: Implementation of etl pipeline that loads data from S3 into statging tables on Redshift, processes them and finally loads them into the Redshift analytics tables
* sql_queries.py: This script contains all SQL statements needed within the above files
* dwh_example.cfg: An example of the configuration file

## Authors and Acknowledgements <a name="Authors-Acknowledgements"></a>
This project has been implemented as part of the Udacity Data Engineering Nanodegree program. The data has been provided by Udacity accordingly as well as the project structure/ file templates.
