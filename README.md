# End-to-End E-Commerce ETL Pipeline with Airflow, Docker, and Postgres

## 📌 Project Overview
This project demonstrates a production-grade **ETL (Extract, Transform, Load) pipeline** designed to ingest e-commerce transaction data. It utilizes **Apache Airflow** for orchestration, **MinIO** (S3-compatible storage) for raw data ingestion, and **PostgreSQL** as the Data Warehouse.

The pipeline is fully containerized using **Docker** and implements robust data engineering practices, including **UPSERT logic** (handling updates and inserts) and automated **Data Quality checks**.

## 🏗 Architecture


1.  **Ingestion:** Raw CSV files are uploaded to an S3-compatible Object Store (MinIO).
2.  **Orchestration:** Airflow triggers a DAG (Directed Acyclic Graph) to detect and process the file.
3.  **Transformation:** Python (Pandas) performs data cleaning, schema validation, and currency calculations.
4.  **Loading:** Cleaned data is loaded into PostgreSQL using an "Upsert" strategy to ensure data consistency.

## 🛠 Tech Stack
* **Containerization:** Docker & Docker Compose
* **Orchestration:** Apache Airflow (2.6+)
* **Object Storage:** MinIO (S3 compatible)
* **Data Warehouse:** PostgreSQL (15)
* **Transformation:** Python 3.9, Pandas, SQLAlchemy
* **Language:** Python, SQL

## ✨ Key Features
* **Containerized Environment:** The entire stack spins up with a single `docker-compose up` command.
* **Robust UPSERT Logic:** Handles duplicate data by updating existing records (e.g., status changes) and inserting new ones.
* **Data Quality Gates:**
    * Schema Validation (JSON Schema)
    * Null/Missing value checks
    * Data type enforcement
* **Automated Analytics:** The final data is query-ready for BI tools.

## 🚀 How to Run

### Prerequisites
* Docker Desktop installed.

### Steps
1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/ecommerce-etl-pipeline.git](https://github.com/YOUR_USERNAME/ecommerce-etl-pipeline.git)
    cd ecommerce-etl-pipeline
    ```

2.  **Start the Services**
    ```bash
    docker-compose up -d
    ```

3.  **Access the Interfaces**
    * **Airflow UI:** `http://localhost:8080` (User/Pass: `admin`/`admin`)
    * **MinIO Console:** `http://localhost:9001` (User/Pass: `minioadmin`/`minioadmin`)

4.  **Trigger the Pipeline**
    * Upload `orders.csv` to the `raw-data` bucket in MinIO.
    * Trigger the `simple_etl_minio_postgres` DAG in Airflow.

5.  **Verify Data**
    Connect to the Postgres database to verify the results:
    ```sql
    SELECT status, COUNT(*), SUM(total_price) 
    FROM orders_clean 
    GROUP BY status;
    ```

## 📊 Sample Data Insights
| Status | Count | Revenue |
| :--- | :--- | :--- |
| Delivered | 22 | $6,608.72 |
| Processing | 11 | $3,301.34 |
| Cancelled | 3 | $1,017.99 |

## 👤 Author
**Rayyan Kauchali**
*MSc Student | Data Science & Engineering*
