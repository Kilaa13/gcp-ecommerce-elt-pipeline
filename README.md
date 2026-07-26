## Project Overview
This project is an automated data pipeline built to process and analyze online shoppers’ purchasing behavior data. The system automatically retrieves raw telemetry data from the data lake, ingests it into the data warehouse, and executes parallel transformations to segment transaction activity and generate monthly revenue reporting views. The entire data workflow is dynamically orchestrated using **Apache Airflow v3** deployed on Google Cloud Platform (GCP) infrastructure.

---

## Data Source
* **Dataset Name:** Online Shoppers Purchasing Intention Dataset
* **Source:** [Kaggle Dataset by Thuan Dao](https://www.kaggle.com/datasets/thuandao/online-shoppers-purchasing-dataset)
* **Description:** Contains feature values belonging to 12,330 online user sessions from an e-commerce platform over a 1-year period to analyze visitor behaviors, page performance (`PageValues`), exit rates, and purchase conversions (`Revenue`).
---

## Technical Stack
* **Programming Language:** Python 3.11
* **Orchestration:** Apache Airflow v3 (Standalone Architecture)
* **Data Lake / Ingestion:** Google Cloud Storage (GCS)
* **Data Warehouse:** Google BigQuery
* **Infrastructure / Cloud:** Google Compute Engine (GCP VM - Debian Linux)
* **Security & Access Control:** GCP IAM & Service Account Credentials
* **Data Transformation:** SQL (BigQuery Dialect)
* **Business Intelligence:** Looker Studio
---

## System & Architecture
The pipeline implements a multi-tier **Medallion Data Architecture** within Google Cloud to transition raw batch data into analytics-ready reporting structures:
```
       [ E-Commerce Raw CSV ]
                 │
                 ▼
   [ Google Cloud Storage (GCS) ] ──► (Bucket: gs://bkt-ecommerce-raw-data/)
                 │
                 ▼
       [ Apache Airflow v3 ]
        (DAG Orchestration)
                 │
                 ├─► BRONZE LAYER  ──► GCS Sensor Validation (check_file_exists)
                 ├─► SILVER LAYER  ──► Load Raw CSV into Staging (staging_data.online_shoppers)
                 └──► GOLD LAYER    ──► Monthly Table Filtering & Revenue Analytics Views
                 │
                 ▼
         [ Google BigQuery ]
       (staging_data Dataset)
                 │
                 ▼
      [ Business Intelligence ]
           (Looker Studio)
```
## Repository Structure
```
gcp-ecommerce-elt-pipeline/
├── dags/
│   └── load_and_transform_view_shoppers.py   # Airflow DAG script
├── architecture/                             # Screenshots and visual assets
│   ├── gcs_bucket.png
│   ├── airflow_dag.png
│   ├── bigquery_tables.png
│   ├── bigquery_query_preview.png
│   └── looker_dashboard.png
└── README.md                                 # Project documentation
```
