# Retail ETL Platform

An end-to-end retail ETL platform that ingests CSV files from Amazon S3 into a source PostgreSQL database, transforms and cleans the data using Python, loads curated datasets into a target PostgreSQL warehouse, and builds analytics-ready staging, intermediate, dimension, fact, and aggregate tables.

## Project Overview

This project simulates a production-style retail data engineering workflow. It handles raw retail data ingestion, cleansing, layered SQL transformations, orchestration, testing, and containerized deployment.

### Key Highlights
- Ingests retail CSV files from Amazon S3
- Loads raw data into source PostgreSQL tables
- Cleans and standardizes data using modular Python transformations
- Loads staging tables into target PostgreSQL
- Builds intermediate, dimension, fact, and aggregate marts for analytics
- Orchestrates pipeline execution with Apache Airflow
- Adds automated testing with Pytest
- Adds CI validation with GitHub Actions
- Dockerized for reproducible local setup

## Architecture

Amazon S3 (CSV files)
→ Source PostgreSQL
→ Python ETL (extract, clean, load)
→ Target PostgreSQL staging tables
→ SQL intermediate models
→ Dimension / Fact / Aggregate marts
→ Airflow orchestration
→ Pytest + GitHub Actions validation

## Tech Stack

- Python
- PostgreSQL
- Apache Airflow
- Docker & Docker Compose
- Pandas
- SQLAlchemy
- Pytest
- GitHub Actions
- Amazon S3
- Boto3

## Repository Structure

```text
retail-etl-platform/
├── .github/workflows/     # CI pipeline
├── dags/                  # Airflow DAGs
├── docker/                # Dockerfiles
├── sql/                   # Source, staging, intermediate, marts SQL
├── src/                   # Python ETL modules
├── tests/                 # Pytest test cases
├── docker-compose.yml     # Multi-service orchestration
├── requirements.txt
└── README.md
