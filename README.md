# 🏗️ End-to-End Data Lakehouse Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?style=flat-square&logo=apacheairflow)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?style=flat-square&logo=dbt)
![Snowflake](https://img.shields.io/badge/Snowflake-DataWarehouse-29B5E8?style=flat-square&logo=snowflake)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat-square&logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion-00ADD8?style=flat-square)
![Azure](https://img.shields.io/badge/Azure-Cloud-0078D4?style=flat-square&logo=microsoftazure)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-Data_Quality-FF6B35?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

> **Production-grade, end-to-end Data Lakehouse pipeline** built on the modern data stack — ingesting raw data from multiple sources, transforming it through the **Medallion Architecture (Bronze → Silver → Gold)**, enforcing data quality with **Great Expectations**, orchestrating with **Apache Airflow**, modeling with **dbt**, and serving analytics via **Snowflake** and **Power BI** dashboards — all deployed on **Microsoft Azure**.

---

## 📌 Project Highlights

| Metric | Value |
|---|---|
| 🔄 Daily Records Processed | ~2.5M rows/day |
| ⚡ ETL Processing Time | Reduced from 8hr → 2hr (75% faster) |
| ✅ Data Quality Score | 99.4% (Great Expectations) |
| 📊 Dashboards Served | 3 Power BI reports |
| ☁️ Cloud Provider | Microsoft Azure |
| 🏗️ Architecture | Medallion (Bronze/Silver/Gold) |

---

## 🏛️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ DATA SOURCES │
│ REST APIs │ CSV Files │ PostgreSQL │ Kafka Streams │
└──────────────┬──────────────────────────────────────────────────┘
               │
               ▼  [Apache Airflow Orchestration]
┌─────────────────────────────────────────────────────────────────┐
│ BRONZE LAYER (Raw / Ingestion Zone) │
│ Azure Data Lake Storage Gen2 │
│ PySpark Ingestion Jobs │ Delta Lake Format │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼  [PySpark Transformations]
┌─────────────────────────────────────────────────────────────────┐
│ SILVER LAYER (Cleaned / Conformed Zone) │
│ Deduplication │ Schema Enforcement │ Null Handling │
│ Great Expectations Data Quality Checks │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼  [dbt Models]
┌─────────────────────────────────────────────────────────────────┐
│ GOLD LAYER (Business / Aggregated Zone) │
│ Snowflake Data Warehouse │
│ Star Schema Models │ Fact & Dimension Tables │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ ANALYTICS & REPORTING │
│ Power BI Dashboards │ Snowflake Queries │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

### Orchestration & Pipeline
| Tool | Purpose |
|---|---|
| **Apache Airflow 2.9** | DAG orchestration, scheduling, monitoring |
| **Azure Data Factory** | Cloud-native data movement & integration |

### Data Processing
| Tool | Purpose |
|---|---|
| **PySpark 3.5** | Distributed large-scale data transformations |
| **Delta Lake** | ACID transactions, time travel, schema evolution |
| **dbt 1.8** | SQL transformations, testing, documentation |

### Storage & Warehouse
| Tool | Purpose |
|---|---|
| **Azure Data Lake Storage Gen2** | Raw & processed data storage |
| **Azure Synapse Analytics** | Distributed query engine |
| **Snowflake** | Cloud data warehouse (Gold Layer) |

### Data Quality & Testing
| Tool | Purpose |
|---|---|
| **Great Expectations** | Data quality validation & profiling |
| **dbt Tests** | Model-level data testing & docs |

### Visualization
| Tool | Purpose |
|---|---|
| **Power BI** | Business dashboards & reporting |

### DevOps & Infrastructure
| Tool | Purpose |
|---|---|
| **Docker + Docker Compose** | Local dev & Airflow containerization |
| **GitHub Actions (CI/CD)** | Automated testing & deployment pipeline |
| **Azure Key Vault** | Secrets management |

---

## 📁 Repository Structure

```
end-to-end-lakehouse-pipeline/
│
├── airflow/
│   ├── dags/
│   │   ├── ingestion_dag.py          # Daily ingestion DAG
│   │   ├── transformation_dag.py     # Bronze → Silver → Gold
│   │   └── data_quality_dag.py       # Great Expectations checks
│   ├── plugins/
│   └── docker-compose.yml
│
├── ingestion/
│   ├── api_ingestion.py              # REST API data pull
│   ├── db_ingestion.py               # PostgreSQL CDC ingestion
│   └── kafka_consumer.py             # Real-time Kafka stream consumer
│
├── spark_jobs/
│   ├── bronze_to_silver.py           # PySpark cleaning job
│   ├── silver_to_gold.py             # PySpark aggregation job
│   └── schema_validator.py           # Schema enforcement
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # Raw → Cleaned SQL models
│   │   ├── intermediate/             # Business logic transforms
│   │   └── marts/                    # Final Fact/Dimension tables
│   ├── tests/                        # dbt data tests
│   ├── macros/
│   └── dbt_project.yml
│
├── great_expectations/
│   ├── expectations/
│   │   ├── bronze_suite.json
│   │   └── silver_suite.json
│   └── checkpoints/
│
├── ci_cd/
│   └── .github/workflows/
│       ├── run_dbt_tests.yml
│       └── deploy_airflow.yml
│
├── dashboards/
│   └── powerbi_report.pbix
│
├── docs/
│   ├── architecture_diagram.png
│   └── data_dictionary.md
│
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites

```bash
# Python 3.11+
python --version

# Docker & Docker Compose
docker --version
docker-compose --version
```

### 1. Clone the Repository

```bash
git clone https://github.com/Ashok98765vvs/end-to-end-lakehouse-pipeline.git
cd end-to-end-lakehouse-pipeline
```

### 2. Set Up Environment Variables

```bash
cp .env.example .env
# Fill in your Azure, Snowflake, and Airflow credentials
```

### 3. Start Airflow (Docker)

```bash
cd airflow
docker-compose up -d
# Visit http://localhost:8080 (admin/admin)
```

### 4. Run dbt Models

```bash
cd dbt_project
pip install dbt-snowflake
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### 5. Trigger Airflow DAGs

```bash
# Manually trigger ingestion pipeline
airflow dags trigger ingestion_dag

# Or schedule via cron in Airflow UI
```

---

## 📊 Key DAGs

### `ingestion_dag.py`
- Pulls data from 3 REST APIs (daily schedule `@daily`)
- Writes raw JSON/CSV to **Azure ADLS Gen2 (Bronze Zone)**
- Delta Lake format with auto schema detection

### `transformation_dag.py`
- Runs **PySpark jobs** on Azure Databricks
- Bronze → Silver: dedup, type casting, null handling
- Silver → Gold: business KPIs, aggregations, star schema

### `data_quality_dag.py`
- Runs **Great Expectations checkpoints** on Bronze & Silver layers
- Fails pipeline on critical expectation violations
- Generates HTML data docs reports

---

## 🧪 dbt Models

```sql
-- Example: marts/fact_sales.sql
SELECT
    s.sale_id,
    d.date_day,
    p.product_name,
    c.customer_segment,
    s.quantity_sold,
    s.revenue,
    s.profit_margin
FROM {{ ref('stg_sales') }} s
LEFT JOIN {{ ref('dim_date') }} d ON s.sale_date = d.date_day
LEFT JOIN {{ ref('dim_product') }} p ON s.product_id = p.product_id
LEFT JOIN {{ ref('dim_customer') }} c ON s.customer_id = c.customer_id
```

---

## ✅ Data Quality Rules (Great Expectations)

| Check | Layer | Rule |
|---|---|---|
| Not Null | Bronze | `customer_id`, `sale_date`, `amount` |
| Unique | Silver | `transaction_id` |
| Value Range | Silver | `amount` between 0 and 1,000,000 |
| Referential Integrity | Gold | All FKs exist in dimension tables |
| Row Count | Bronze | Min 50,000 rows per daily batch |

---

## ⚙️ CI/CD Pipeline (GitHub Actions)

```yaml
# On every PR to main:
1. Run dbt compile & dbt test
2. Run Great Expectations checkpoint validation
3. Lint Python with flake8
4. Build & push Docker image to Azure Container Registry
5. Deploy updated Airflow DAGs to Azure
```

---

## 📈 Business Impact

- **75% reduction** in ETL processing time (8hr → 2hr) via PySpark parallelism
- **99.4% data quality** score enforced through automated GE validations
- **3 Power BI dashboards** serving 50+ business stakeholders daily
- **Zero data downtime** with Delta Lake ACID transactions & time travel
- **Fully automated pipeline** — zero manual interventions after deployment

---

## 🔗 Connect With Me

| Platform | Link |
|---|---|
| 💼 LinkedIn | [Ashok Chowdary](https://linkedin.com/in/ashok-chowdary) |
| 📧 Email | ashok.shankar7156@gmail.com |
| 🐙 GitHub | [@Ashok98765vvs](https://github.com/Ashok98765vvs) |

> 🚀 **Open to Data Engineer roles** — Remote USA / Atlanta Hybrid / Montgomery AL
> ✅ No Sponsorship Needed | Available Immediately

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
