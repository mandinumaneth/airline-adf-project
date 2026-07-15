# ✈️ Airline Data Engineering Pipeline — Azure Data Factory

An end-to-end batch data engineering pipeline built entirely on **Azure Data Factory (ADF)** that ingests airline data from three different types of sources, cleans and conforms it, and serves curated business-ready insights — following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 📌 Project Overview

This project simulates a real-world airline reporting system. It pulls **passenger, flight, airline, airport, and booking data** from heterogeneous sources (on-prem files, a relational database, and a REST API), lands it in a **Bronze** layer, cleans/transforms it into a **Silver** layer of Delta tables, and finally aggregates it into a **Gold** layer that powers a business KPI: **Top 5 airlines by ticket revenue**.

The whole pipeline is orchestrated, parameterized, scheduled, and monitored — mirroring how data engineering teams build production ingestion frameworks in Azure.

---

## 🏗️ Architecture

```
                ┌─────────────────────┐
On-Prem (SHIR)  │  DimPassenger.csv    │
                │  DimAirline.csv      ├──┐
                │  DimFlight.csv       │  │
                └─────────────────────┘  │
                                          │
Azure SQL DB    ┌─────────────────────┐  │      ┌──────────┐      ┌──────────┐      ┌──────────┐
(Incremental)   │  FactBookings       ├──┼─────▶│  BRONZE  ├─────▶│  SILVER  ├─────▶│  GOLD    │
                └─────────────────────┘  │      │ (raw)    │      │ (Delta,  │      │ (Delta,  │
                                          │      └──────────┘      │ cleaned) │      │ curated) │
REST API        ┌─────────────────────┐  │                        └──────────┘      └──────────┘
(GitHub raw     │  DimAirport.json    ├──┘
 JSON)          └─────────────────────┘

Storage: Azure Data Lake Storage Gen2  |  Format: Delta Lake  |  Orchestration: Azure Data Factory
```

- **Bronze** – raw ingested data, format-as-is (CSV / JSON / Parquet)
- **Silver** – cleaned, standardized, deduplicated Delta tables (one per dimension/fact)
- **Gold** – business-level aggregate ready for reporting/BI

---

## 📂 Data Sources Ingested

| # | Source Type | Data | How it's ingested |
|---|---|---|---|
| 1 | **Simulated On-Premises** | `DimPassenger.csv`, `DimAirline.csv`, `DimFlight.csv` | `ForEach` activity looping over a parameterized file list + `Copy` activity |
| 2 | **Azure SQL Database** | `FactBookings` | Incremental (delta) load using a **watermark pattern** — only new bookings since the last successful run are copied |
| 3 | **REST API** | `DimAirport` | `Web Activity` GET request to a public raw JSON endpoint on GitHub, followed by a `Copy` activity (JSON → JSON) |


> **Note:** A Self-Hosted Integration Runtime was configured in the factory, but since I didn't have 
> an actual on-premises server available, the "on-prem" source was simulated using files stored in 
> Azure Data Lake Storage instead of a real on-prem machine. The pipeline design (ForEach + 
> parameterized ingestion) is built the same way it would be for a genuine on-prem source.
---

## ⚙️ Pipelines

| Pipeline | Purpose |
|---|---|
| **Parent Pipeline** | Master orchestrator. Runs `onprem_ingestion` → `API_Ingestion` → `SQLToDatalake` sequentially (each depends on the previous succeeding), then calls a **Logic App webhook** to report run status/errors |
| **onprem_ingestion** | Iterates over a parameterized array of file names with `ForEach`, copying each simulated on-prem CSV into the Bronze zone |
| **API_Ingestion** | Calls a public REST API with `Web Activity`, then copies the JSON response into the lake with column-level mapping |
| **SQLToDatalake** | Implements **incremental extraction** from Azure SQL: looks up the last watermark, looks up the latest available date, copies only the delta as Parquet, then updates the watermark file for the next run |
| **SilverLayer** | Triggers the `DataTransformation` Mapping Data Flow (Bronze → Silver) |
| **GoldLayer** | Triggers the `DataServing` Mapping Data Flow (Silver → Gold) |

**Activities used across pipelines:** `Copy`, `Web`, `Lookup`, `ForEach`, `ExecutePipeline`, `ExecuteDataFlow`, with parameters, expressions, and activity-dependency chaining throughout.

---

## 🔄 Mapping Data Flows

### 1. Silver Layer — `DataTransformation`
Reads all 5 Bronze sources and applies targeted cleansing per entity:

- **DimAirline** → standardize `country` to uppercase
- **DimFlight** → rename/select columns into clear timestamp names (`departure_timestamp`, `arrival_timestamp`)
- **DimPassenger** → map `gender` → `gender_flag`, replace `M`/`F` with `Male`/`Female` via `regexReplace`, filter to `age > 25`, derive `first_name` by splitting `full_name`
- **FactBookings** → cast `ticket_cost` to integer (with error handling)
- **DimAirport** → lowercase `airport_name`

All five branches pass through an `alterRow` (**upsert**) transformation and land as **Delta tables** in the `silver` filesystem, each keyed on its natural business key (`airline_id`, `flight_id`, `passenger_id`, `booking_id`, `airport_id`). `allowSchemaDrift` is enabled throughout to gracefully absorb upstream schema changes.

### 2. Gold Layer — `DataServing`
Builds a curated, BI-ready business view:

1. **Join** `FactBookings` with `DimAirline` on `airline_id` (left join)
2. **Select** the relevant columns
3. **Aggregate** — `groupBy(airline_name)`, `Total_Sales = sum(ticket_cost)`
4. **Window** — `denseRank()` over `Total_Sales` descending
5. **Filter** — keep `Top < 6` (Top 5 airlines)
6. **Insert (overwrite)** into a Delta table: `businessView1_TopAirline`

---

## 🔗 Linked Services & Integration Runtimes

- **Azure SQL Database** — source for the transactional `FactBookings` table
- **Azure Data Lake Storage Gen2** — used for both the Bronze/Silver/Gold lakehouse zones and to simulate the "on-prem" file drop
- **HTTP Linked Service** — anonymous connection to `raw.githubusercontent.com` as a mock REST API
- **Self-Hosted Integration Runtime (SHIR)** — represents the on-premises network boundary for the simulated on-prem ingestion

---

## ⏱️ Scheduling & Monitoring

- **Schedule Trigger** — runs the Parent Pipeline daily on a recurrence schedule (timezone-aware)
- **Failure Alerting** — a `Web Activity` posts the pipeline name, run ID, status, and error message (if any) to an **Azure Logic App** HTTP endpoint at the end of every run, enabling automated notifications on failure

---

## 🧠 Concepts, Tools & Skills Covered

- Core Azure Data Factory components: **Pipelines, Activities, Datasets, Linked Services, Triggers, Integration Runtimes**
- **Self-Hosted Integration Runtime** to simulate hybrid/on-premises connectivity
- **Copy Activity** across multiple formats and stores: Delimited Text (CSV), JSON, Parquet, Azure SQL, Delta
- **Web Activity** for calling external REST APIs (GET) and Logic App webhooks (POST)
- **Lookup Activity** for retrieving control-table/watermark values
- **ForEach Activity** for iterating over a parameterized list of files
- **Execute Pipeline Activity** and the **parent-child pipeline** orchestration pattern
- **Pipeline and dataset parameterization** for reusable, dynamic pipelines
- **Incremental (delta) loading** using the watermark design pattern
- **Dynamic content & expressions** (`@pipeline()`, `@activity()`, `@item()`, expression builder)
- **Mapping Data Flows** and their transformations: `derive`, `select`, `filter`, `cast`, `join`, `aggregate`, `window` (ranking with `denseRank()`), `alterRow`
- **Schema drift handling** (`allowSchemaDrift`)
- **Medallion Architecture** design (Bronze → Silver → Gold)
- **Delta Lake** format for ACID-compliant, upsertable lake tables
- Building **curated business views** (Top-N ranking & aggregation) for reporting
- Data Flow compute configuration (core count, compute type, trace level) for debug/execution
- **Schedule Triggers** and recurrence/timezone configuration
- **Operational monitoring** — pipeline failure alerting via Logic Apps
- Working with **Azure Data Lake Storage Gen2** hierarchical namespace
- ADF **Git integration** — exporting the factory as versioned ARM-style JSON (`factory/`, `pipeline/`, `dataset/`, `linkedService/`, `dataflow/`, `trigger/`, `integrationRuntime/`)

---

## 🛠️ Tech Stack

`Azure Data Factory` · `Azure Data Lake Storage Gen2` · `Azure SQL Database` · `Delta Lake` · `Azure Logic Apps` · `Self-Hosted Integration Runtime`

---

## 📁 Repository Structure

```
airline-adf-project/
├── factory/               # ADF factory-level definition
├── linkedService/         # Connections to SQL, Data Lake, GitHub, on-prem
├── dataset/                # Reusable/parameterized dataset definitions
├── pipeline/               # Orchestration pipelines
├── dataflow/                # Mapping Data Flows (Silver + Gold transformations)
├── trigger/                # Schedule trigger
└── integrationRuntime/      # Self-hosted IR definition
```

---

## 📸 Pipeline Execution Evidence

**All pipelines running successfully via the daily schedule trigger:**

<img width="1919" height="427" alt="image" src="https://github.com/user-attachments/assets/01984fa7-959d-44e6-acc5-7d9637ad3a25" />

**Trigger run history — `triggerIngest` firing on schedule:**

<img width="1919" height="384" alt="image" src="https://github.com/user-attachments/assets/a61daa81-adb6-465b-b928-2d7bf838a4d9" />

---

## 📚 Learning Reference

This project was built by following along with the YouTube walkthrough
[Azure Data Factory End-To-End Project With Azure DevOps | 2025 Zero To Pro Guide](https://youtu.be/Za_9XYwPbKM),
then extended and adapted with my own configuration, naming, and pipeline structure.

---
