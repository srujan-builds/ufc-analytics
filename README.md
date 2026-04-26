# UFC Analytics Data Pipeline

An end-to-end, fully automated data engineering pipeline that extracts live UFC event and fighter data, transforms it using dimensional modeling, and loads it into a Snowflake data warehouse for downstream analytics.

## Tech Stack

* **Data Extraction:** Python (BeautifulSoup, Requests)
* **Orchestration:** Apache Airflow (Dockerized)
* **Data Warehouse:** Snowflake
* **Transformation:** dbt (Data Build Tool)
* **Security:** RSA Key-Pair Authentication
* **Containerization:** Docker & Docker Compose


## Key Features

* **Incremental Delta Loads:** The pipeline is designed to identify and extract only the latest events (e.g., pulling exactly 13 fights from a weekend event without duplicating historical data).
* **Dimensional Data Modeling:** Built from business requirements first. The dbt marts layer takes standard fight records and fans them out (1 fight = 2 distinct corner records) to provide granular, fighter-perspective statistics.
* **Custom Analytics Flags:** Engineered custom logic (like an is_winner flag) to instantly calculate historical win/loss ratios, double-champ status, and strike differentials.
* **Secure Auth:** Bypasses basic password authentication in favor of secure, multiline RSA Key-Pair authentication for both the Python connector and dbt profiles.

## Project Structure

```text
├── dags/
│   ├── extract/              # Python extraction scripts (main.py, scraper.py, db_connector.py)
│   ├── dbt_ufc/              # dbt project folder (models, macros, profiles.yml)
│   └── ufc_pipeline_dag.py   # Airflow DAG definition
├── Dockerfile                # Custom Airflow image build with requirements
├── docker-compose.yaml       # Airflow cluster configuration
├── requirements.txt          # Python dependencies
└── .gitignore

