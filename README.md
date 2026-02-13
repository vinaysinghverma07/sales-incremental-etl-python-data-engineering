# 🚀 Sales ETL Project – Production-Grade Data Engineering Pipeline

## 📌 Overview
This project demonstrates a production-ready ETL pipeline built using Python and PostgreSQL with enterprise-level features including:
* **Incremental loading** (Watermark logic)
* **Data Quality** validation
* **UPSERT handling** (ON CONFLICT)
* **Centralized logging**
* **Modular architecture**
* **Cloud migration readiness** (Databricks / ADF)

This project reflects real-world data engineering practices aligned with industry experience.

## 🏗️ Architecture Overview
```text
Raw CSV Data
     ↓
Extract Layer
     ↓
Transform Layer
     ↓
Data Quality Checks
     ↓
Incremental Filter (Watermark)
     ↓
UPSERT into PostgreSQL
```

📁 Project Structure
```text
sales_etl_project2/
│
├── data/
│   └── raw/
│       └── sales_data_large_1000.csv
│
├── logs/
│   └── etl.log
│
├── src/
│   ├── utils.py
│   ├── extract.py
│   ├── transform.py
│   ├── quality.py
│   ├── incremental.py
│   ├── load_postgres.py
│   └── main.py
│
├── tests/
│   ├── test_transform.py
│   ├── test_incremental.py
│   └── test_quality.py
│
├── config.yaml
└── README.md
```

## ⚙️ Key Features Implemented

### 1️⃣ Extract Layer
* Reads raw CSV files safely
* Handles malformed structures
* Validates schema presence
* Logs extraction metrics

**Uses:**
* `pandas`
* `pathlib`
* Centralized logging

### 2️⃣ Transform Layer
Applies business logic:
* Converts string dates to datetime
* Converts numeric fields
* Handles missing values
* Removes duplicates
* Creates derived column (`revenue = quantity * price`)

**Designed using:**
* Vectorized Pandas operations
* Defensive programming practices

### 3️⃣ Data Quality Checks
Pipeline fails if:

| Check Type | Rule |
| :--- | :--- |
| **Null Check** | Mandatory fields must not be null |
| **Range Check** | `quantity > 0`, `price > 0` |
| **Duplicate Check** | No duplicate `order_id` |
| **Schema Validation** | Enforced datatype validation |

**Fail-fast design ensures:**
* Bad data never reaches the warehouse.

### 4️⃣ Incremental Load (Watermark Logic)
Instead of loading all data every run:
* Fetches `MAX(order_date)` from PostgreSQL
* Filters incoming dataframe
* Loads only new records

**Incremental Pattern:**
```python
df = df[df["order_date"] > last_watermark]
```

**Benefits:**
* Faster loads
* Database friendly
* Idempotent reruns
* Production-ready behavior

### 5️⃣ UPSERT Implementation (ON CONFLICT)
To ensure safe reruns and handle late-arriving updates:
* Data first loads into a staging table
* `INSERT ... ON CONFLICT DO UPDATE` merges into target table
* Staging table is dropped after merge

**PostgreSQL Merge Pattern:**
```sql
ON CONFLICT (order_id) 
DO UPDATE SET 
    order_date = EXCLUDED.order_date,
    customer_id = EXCLUDED.customer_id,
    product = EXCLUDED.product,
    quantity = EXCLUDED.quantity,
    price = EXCLUDED.price,
    revenue = EXCLUDED.revenue;
```

**Ensures:**
* No duplicate primary keys
* Safe reprocessing
* Enterprise-grade load strategy

## 🪵 Logging Strategy

Logs are written to:
* **Console:** Real-time monitoring during execution
* **logs/etl.log:** Persistent audit log for historical tracking

**Features:**
* **Centralized logger configuration:** Unified format across all modules
* **`logger.exception()`:** Captures full stack traces for easier debugging
* **Step-level logging:** Tracks progress of Extract, Transform, and Load phases
* **Failure-level logging:** Immediate alerts for data quality or connection issues

**Example:**
`2026-02-06 14:37:53 - INFO - Last watermark fetched: 2024-12-31`


## 🧪 Unit Testing (Pytest)

Tests included for:
* **Revenue calculation:** Validates mathematical correctness
* **Incremental filtering logic:** Ensures only delta data is captured
* **Data quality rules:** Confirms validation checks block bad records
* **Transformation validation:** Checks schema and data type consistency

**Run tests:**
```bash
pytest


** Why this matters: **
* Prevents regression
* Enables safe refactoring
* Demonstrates production discipline


## 🗄️ PostgreSQL Setup

Ensure table exists:

```sql
CREATE TABLE data.sales_orders (
    order_id INT PRIMARY KEY,
    order_date TIMESTAMP,
    customer_id INT,
    product TEXT,
    quantity INT,
    price NUMERIC,
    revenue NUMERIC
);
```

## ▶️ How to Run the Pipeline

```bash
python src/main.py
```

**Ensure:**
* Python 3.10+
* PostgreSQL running locally
* Correct credentials in config.yaml

## 📊 Technologies Used

| Technology | Purpose |
| :--- | :--- |
| **Python 3.10+** | Core programming language |
| **pandas** | Data manipulation |
| **SQLAlchemy** | Database connectivity |
| **PostgreSQL** | Target warehouse |
| **PyYAML** | Configuration management |
| **logging** | Centralized logging |
| **pytest** | Unit testing |


## ☁️ Cloud Migration Ready

This architecture maps directly to enterprise cloud stacks such as **Azure Data Factory (ADF)**, **Databricks (Delta Lake)**, and **Apache Airflow**.

### Mapping:

| Local Python | Cloud Equivalent |
| :--- | :--- |
| **Extract** | ADF Copy Activity |
| **Transform** | Databricks Notebook |
| **Quality Checks** | Databricks Expectations |
| **Incremental** | Lookup Activity |
| **UPSERT** | Delta Merge |

## 📈 What This Project Demonstrates

*   ✅ **Incremental ETL design**
*   ✅ **Production-grade logging**
*   ✅ **Data Quality enforcement**
*   ✅ **Idempotent loads**
*   ✅ **Staging-to-target merge strategy**
*   ✅ **Unit testing in data pipelines**
*   ✅ **Cloud-ready architecture**

## 🧭 Next Steps (Planned Enhancements)

*   **Databricks** full implementation
*   **Azure Data Factory** orchestration
*   **Slowly Changing Dimensions** (SCD Type 2)
*   **Audit table** tracking
*   **Dockerization**
*   **CI/CD pipeline** setup

## Getting Started

### Prerequisites
- Python and pandas library installed
- PostgreSQL Installed
- Git installed locally

### Installation
Clone this repository:

```bash
   git clone https://github.com/vinaysinghverma07/sales-incremental-etl-python-data-engineering.git
