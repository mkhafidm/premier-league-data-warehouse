# Premier League Data Warehouse

**An End-to-End Data Warehouse pipeline for Premier League / Fantasy Premier League (FPL) data.**
This project includes automated Extract, Load, and Transform (ELT) workflows and produces clean, structured analytical tables ready for consumption by business intelligence (BI) dashboards.

---

## Tech Stack

This project is built using modern cloud data stack principles:

* **Python:** Used for the **ETL** script (fetching raw data from the FPL API and loading it into BigQuery).
* **Google BigQuery:** Serves as the scalable and serverless **Data Warehouse**.
* **dbt (Data Build Tool):** Manages the entire **Transformation (T)** phase, handling all SQL logic (staging $\rightarrow$ dimension $\rightarrow$ fact $\rightarrow$ mart).
* **Power BI:** Used for data visualization and building the final analytical dashboards.
* **GitHub:** Used for Version Control and slated for future **CI/CD** automation (via GitHub Actions).

---

## Project Structure

The structure logically separates the data acquisition (ETL) and the data modeling (dbt).

```bash
premier-league-dwh/
│
├── etl/                          # Extract & Load (Python)
│   └── extract_load.py
│
├── dbt/                          # dbt project (Transform)
│   ├── models/
│   │   ├── staging/          
│   │   ├── dimensions/         
│   │   ├── facts/               
│   │   └── marts/               
│   ├── dbt_project.yml
│   └── packages.yml
│
├── dashboards/                   # BI & analytics outputs
│
├── .github/
│   └── workflows/
│       └── main.yml           
│
├── requirements.txt
├── README.md
└── .gitignore

▶️ How to Run Locally
Follow these steps to set up the environment, load the raw data, and execute the dbt transformations.

1. Environment Setup
Install all necessary Python dependencies:

Bash

pip install -r requirements.txt
2. Configure GCP Credentials
Set your environment variables for BigQuery access (replace placeholders with your actual values).

Bash

export GCP_PROJECT_ID="premier-league-analysis"
export GCP_BQ_DATASET="fpl_data"
# Path to your GCP Service Account Key file
export GCP_SERVICE_ACCOUNT_KEY='/path/to/premier-league-analysis-key.json' 
3. Execute ETL (Extract & Load)
Run the Python script to fetch the latest data and load it into your BigQuery raw tables:

Bash

python etl/extract_load.py
4. Execute dbt Transformations
Navigate to the dbt project folder and run the models:

Bash

cd dbt
# Download dbt packages (if applicable)
dbt deps
# Execute all SQL models (Staging, Dim, Fact, Mart)
dbt run
# Run data quality and schema tests
dbt test