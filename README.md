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

AWS: 
0. Create IAM user (dwhadmin) in the AWS management console 

Files:
1. Add AWS KEY and SECRET of the dwhadmin user into dwh_example.cfg and save it under dwh.cfg
2. Run create_cluster.py to create an AWS Redshift data warehouse
3. Add DWH_ENDPOINT and DWH_ROLE_ARN into dwh.cfg (they get logged in step 2)
4. Run create_tables.py to create the tables in AWS Redshift
5. Run etl.py to load data from staging tables to analytics tables on Redshift

**Make sure to delete your redshift cluster afterwards if not needed anymore to prevent unnecessary costs.**

## File Descriptions <a name="File-Descriptions"></a>

* create_cluster.py: Creates the AWS Redshift data warehouse and dwh role arn 
* create_tables.py: Creates the fact and dimension tables defined in sql_queries.py in Redshift
* etl.py: Implements the etl pipeline that loads data from S3 into statging tables on Redshift, processes them and finally loads them into the Redshift analytics tables
* sql_queries.py: Contains all SQL statements needed within the above files
* dwh_example.cfg: An example of the configuration file

## Authors and Acknowledgements <a name="Authors-Acknowledgements"></a>
This project has been implemented as part of the Udacity Data Engineering Nanodegree program. The data has been provided by Udacity accordingly as well as the project structure/ file templates.
