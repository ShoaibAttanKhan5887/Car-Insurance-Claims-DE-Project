# Smart Claims Processing & Unified Data Platform

## Overview
This project establishes a unified data platform on Azure Databricks to create a single source of truth for insurance operations and automate the claims handling process. By leveraging a Medallion architecture and integrating advanced Computer Vision capabilities, the system accelerates claim resolution, improves accuracy, and provides real-time operational insights.

## Key Objectives
***Unified Data Ecosystem***: Centralized repository for Customer, Claims, Policy, and Telematics data.

***Smart Claim Automation***: Integration of Computer Vision to assess car damage severity (Major/Minor) from images.

***Automated Validation***: Automated rule-based checks for policy validity dates and claim amount thresholds to accelerate processing speed.

## Architecture & Data Flow
The platform is built on a structured Databricks Medallion Architecture, moving data logically from ingestion to consumption.
<img width="864" height="486" alt="Architecture CarInsurance" src="https://github.com/user-attachments/assets/fd182c08-ce35-48f5-9fa9-7d1021914490" />


**1. Source Systems & Ingestion**
*Relational Databases:* Customer and policy details are securely ingested using Lakeflow Connect.
*Azure Event Hubs:* Real-time streaming of telematics data is captured and processed through Lakeflow Declarative Pipelines.
*Azure Data Lake Storage:* Car damage images and raw files are continuously ingested using Databricks Auto Loader.

**2. Processing & Storage**
*Medallion Architecture:* Raw data is cleansed, enriched, and aggregated through multi-hop Delta tables (Bronze, Silver, Gold).
*Orchestration:* End-to-end workflows are automated and monitored utilizing Databricks Jobs and Lakeflow Declarative Pipelines.
*Model Management:* Computer Vision models utilized for damage assessment are tracked and versioned via MLflow.

**3. Consumption & Serving**
*Mosaic AI Serving:* Hosts the Computer Vision models to provide real-time inference for claim severity checks.
*DBSQL & AI/BI Dashboards:* Exposes curated Gold-layer data to business intelligence tools, enabling AI/BI Genie and downstream applications to query metrics instantly.


## Technology Stack
**Platform:** Azure Databricks, Delta Lake\\
**Ingestion:** Auto Loader, Lakeflow Connect, Azure Event Hubs\\
**Storage:** Azure Data Lake Storage (Blob Storage)\\
**AI & Machine Learning:** Mosaic AI Serving, MLflow, Computer Vision\\
**Analytics & Serving:** Databricks SQL (DBSQL), AI/BI Dashboards\\


