# Incremental Sales Data Pipeline using ADF and Databricks

## Overview

This project demonstrates an end-to-end data engineering solution for a fictional e-commerce company, BuyAnything Sales. The solution is designed to efficiently ingest, process, and organize sales order data using Azure Data Factory (ADF), Azure Data Lake Storage Gen2 (ADLS Gen2), Azure SQL Database, and Azure Databricks.

The pipeline implements incremental data loading to process only newly available records, reducing data movement and improving efficiency. Following the Medallion Architecture (Bronze, Silver, and Gold layers), raw sales data is ingested from an API, transformed into a cleansed and standardized format, and ultimately modeled into fact and dimension tables for downstream analytical consumption.

The project demonstrates key data engineering concepts including API-based ingestion, incremental ETL processing, orchestration with Azure Data Factory, data transformation with PySpark, and dimensional data modeling using a star schema approach.

## Architecture

### Bronze Layer

ADF ingests sales order data from an external API and stores it in Azure Data Lake Storage Gen2 in Parquet format as raw data.

### Silver Layer

Azure Databricks reads the raw data from the Bronze layer, performs data cleansing and transformations, and stores the refined dataset in the Silver layer.

### Gold Layer

Azure Databricks creates analytics-ready dimension and fact tables from the Silver layer data in delta format.

Dimension Tables:
- DimCustomer
- DimProduct
- DimDate
- DimSalesRegion

Fact Table:
- FactSales

The dimensional model follows a star schema design to support downstream analytical workloads.

## Pipeline Flow

### Pipeline 1: API Ingestion Pipeline

The first Azure Data Factory pipeline retrieves the complete sales dataset from an API and loads it into Azure SQL Database.

### Pipeline 2: Incremental Load Pipeline

The second pipeline implements incremental loading using a DateHolder table. It:

* Retrieves the last processed date using a Lookup activity.
* Identifies newly available records in Azure SQL Database.
* Copies only incremental data to the Bronze layer in ADLS Gen2.
* Stores the data in Parquet format.
* Updates the DateHolder table using a Stored Procedure activity.

### Pipeline 3: Databricks Processing Pipeline

The third pipeline orchestrates Azure Databricks notebooks to process the data through the Medallion Architecture:

1. Reads raw data from the Bronze layer.
2. Performs data cleansing and transformation operations.
3. Writes the refined dataset to the Silver layer.
4. Creates dimension tables (Customer, Product, Date, and Sales Region).
5. Builds the FactSales table by joining the dimension tables with sales transaction data.
6. Stores the final dimensional model in the Gold layer.

## Technologies Used

- Azure Data Factory (ADF)
- Azure Databricks
- Azure Data Lake Storage Gen2 (ADLS Gen2)
- Azure SQL Database
- PySpark
- SQL
- Delta Lake
- Parquet
- Medallion Architecture

## Key Features

- Incremental Data Loading using a DateHolder table
- API-Based Data Ingestion
- Automated Pipeline Orchestration with Azure Data Factory
- Bronze-Silver-Gold (Medallion) Architecture
- Fact and Dimension Data Modeling (Star Schema)
- Delta Tables for Gold Layer Storage
- Parquet-Based Storage for Bronze and Silver Layers
- Data Transformation and Cleansing using PySpark
- Scalable Data Processing with Azure Databricks


