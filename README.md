# STEDI-Human-Balance-Lakehouse

## Overview

This project utilizes AWS Glue, AWS S3, Python, and Apache Spark to build a lakehouse solution that meets specific data processing requirements set by STEDI data scientists. The solution involves the creation and manipulation of Glue tables for different data sources, with a focus on data sanitization and aggregation. 

## Table of Contents

- [Requirements](#requirements)
- [Athena Queries](#athena-queries)
- [Screenshots](#screenshots)
- [Glue jobs](#glue-jobs)


## Requirements

1. **Copy data**:
   - Ingest data using AWS powerShell using the following commad:
     ```bash
     cd STEDI-Human-Balance-Lakehouse/customer/
     aws s3 cp s3://your-bucket/customer/landing

2. **Glue Tables Creation**:
   - Create three Glue tables for the landing zones: 
     - `customer_landing`
     - `step_trainer_landing`
     - `accelerometer_landing`
   - The create statements for this tables are included in ddlScripts directory.

3. **Data Sanitization Jobs**:
   - Create two Glue jobs to sanitize data:
     - `customer_trusted`: Only includes customers who agreed to share data.
     - `accelerometer_trusted`: Only includes accelerometer readings from consenting customers.

4. **Data Quality Verification**:
   - Verify the success of the Glue jobs by querying the `customer_trusted` table using Athena.

5. **Curated Data Preparation**:
   - Create a Glue job to generate the `customers_curated` table, including customers with accelerometer data.

6. **Step Trainer Data Processing**:
   - Implement two Glue jobs:
     - `step_trainer_trusted`: Contains step trainer records for customers who agreed to share data.
     - `machine_learning_curated`: Aggregated table for step trainer readings and associated accelerometer readings.

## Athena Queries
Once tables are created, run the Queries to verify data, example:

```sql
select * from customer_trusted
```

Check your work!
After each stage of the project, check if the row count in the produced table is correct. You should have the following number of rows in each table:

-Landing:
 - Customer: 956
 - Accelerometer: 81273
 - Step Trainer: 28680

-Trusted:
 - Customer: 482
 - Accelerometer: 40981
 - Step Trainer: 14460
 
-Curated:
 - Customer: 482
 - Machine Learning: 43681

## Screenshots
Screenshots of running queries results in Athena are included in the screenshots directory.

## Glue jobs
The Glue jobs python code are included in the pythonScripts directory.

## Notes
This project is an excersice from Udacity, the data was obtained from the the udacity [repository](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/) 
