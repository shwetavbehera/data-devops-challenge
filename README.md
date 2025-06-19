# Data & DevOps Engineering Challenge

This project is a full-stack data pipeline that ingests, cleans, transforms, and aggregates banking data using Apache Spark on AWS Glue. Infrastructure is provisioned via Terraform and deployed through a GitHub Actions CI/CD pipeline.

---

## Project Overview

- **Input**: Retail banking CSV files (clients, accounts, transactions, loans, etc.)
- **Goal**: Clean data, calculate average loan amount per district, and output to Parquet
- **Stack**: PySpark, AWS Glue, S3, Terraform, GitHub Actions

---

## Download Data
[Here is the Google drive link to the data.](https://drive.google.com/file/d/1XAC-bK29qppHwQHrwVGBa4yCH6ocD7iL/view)

## Cloud Deployment with Terraform

Prerequisites: AWS CLI configured, Terraform installed

```
# 1. Deploy infrastructure
terraform init
terraform apply

# 2. Upload locally downloaded data to S3
aws s3 cp "<absolute path to your data folder>" s3://data-devops-challenge-source-data/raw/ --recursive --exclude "*" --include "*.csv" --region eu-central-1

# 3. Trigger Glue Crawler
 aws glue start-crawler   --name devops-crawler   --region eu-central-1

# 4. Trigger Glue ETL Job
aws glue start-job-run   --job-name devops-etl-job   --region eu-central-1
```
