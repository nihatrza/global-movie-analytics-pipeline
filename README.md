# 🎬 Global Movie Analytics — Enterprise Data Engineering & Power BI Executive Dashboard

[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=power-bi&logoColor=black)](https://powerbi.microsoft.com/)
[![Google BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=google-bigquery&logoColor=white)](https://cloud.google.com/bigquery)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Oracle Cloud](https://img.shields.io/badge/Oracle_Cloud_Infrastructure-F80000?style=for-the-badge&logo=oracle&logoColor=white)](https://www.oracle.com/cloud/)

An automated **End-to-End Enterprise Data Pipeline & Interactive Decision-Support System** analyzing global film market performance from **2000 to 2026**. This project integrates real-time daily ingestion from **The Movie Database (TMDB) REST API**, dynamic orchestration via **Apache Airflow**, data warehousing in **Google BigQuery** (Star Schema), analytical SQL transformations, and an executive-level **3-page Power BI dashboard** with custom dark UI/UX design (`#0F172A`) and advanced DAX modeling.

---

## 📌 Executive Summary & Business Impact

In the film and entertainment industry, evaluating investment potential requires robust commercial metrics rather than unverified popularity indices. Legacy analytics platforms often suffer from distorted financial KPIs caused by incomplete dataset entries (e.g., films with unrecorded budgets or box-office revenues represented as `$0`).

### Key Business Metrics (Verified Dataset Baseline)
* **Analyzed Commercial Releases:** **6,655** deduplicated films (Strictly filtered by `budget > 0` AND `revenue > 0`).
* **Cumulative Global Box-Office Revenue:** **$707 Billion**
* **Cumulative Net Industry Profit:** **$469 Billion**
* **Portfolio Profitability Margin:** **66.35%** Net Profit Margin.
* **Portfolio Outcome Balance:** **73.36% Profitable** vs. **26.64% Loss-making** commercial releases.
* **Average Return on Investment (ROI):** **421.9%** across all production tiers.

---

## 🏗️ Technical Architecture & Workflow

```text
                                 [ DATA SOURCES ]
                                        │
                             TMDB REST API v3 / v4
                                        │
                                        ▼
                         [ ORCHESTRATION & INGESTION ]
                Apache Airflow DAG (Oracle Cloud Infrastructure)
                                        │
                                        ▼
                           [ PYTHON ETL PIPELINE ]
                      · Extractor: Dynamic API Pagination
                      · Cleaning: Data Quality Gate (`budget > 0 AND revenue > 0`)
                      · Deduplication: Windowing & Popularity Scoring
                                        │
                                        ▼
                         [ DATA WAREHOUSE & MODELING ]
                   Google BigQuery (`tmdb-movie-analytics.tmdb_analytics`)
                      ├── Star Schema (Fact, Dims, Bridge Tables)
                      └── Analytical Layer (`vw_fact_movies_unique`)
                                        │
                                        ▼
                        [ BUSINESS INTELLIGENCE LAYER ]
                Power BI Desktop / Service (DirectQuery & Import)
                      ├── Advanced DAX Measures & Dynamic Formats
                      ├── Logarithmic Scatter Scale Adjustments
                      └── 3-Tier Navigation Executive View
```

---

## 🗄️ Data Warehouse Design (BigQuery Star Schema)

The analytical data warehouse is structured using a **Star Schema architecture** with bridge tables handling many-to-many relationships (e.g., films with multiple cast members, directors, genres, or production companies).

```text
                      ┌──────────────────────┐
                      │    dim_directors     │
                      └──────────┬───────────┘
                                 │ 1
                                 │
                                 │ N
                      ┌──────────┴───────────┐
                      │bridge_movie_directors│
                      └──────────┬───────────┘
                                 │ N
                                 │
                                 │ 1
┌─────────────────┐   ┌──────────┴───────────┐   ┌──────────────────┐
│   dim_genres    ├───┤  bridge_movie_genres ├───┤   fact_movies    │
└─────────────────┘1 N└──────────────────────┘N 1└─────────┬────────┘
                                                           │ 1
                                                           │
                                                           │ N
                                                ┌──────────┴───────────┐
                                                │ bridge_movie_cast    │
                                                └──────────┬───────────┘
                                                           │ N
                                                           │
                                                           │ 1
                                                ┌──────────┴───────────┐
                                                │       dim_cast       │
                                                └──────────────────────┘
```

### Table Specifications (`tmdb-movie-analytics.tmdb_analytics`)

| Table Name | Entity Type | Grain / Primary Key | Description |
| :--- | :--- | :--- | :--- |
| `fact_movies` | Fact Table | `movie_id` | Core metrics (Budget, Revenue, Profit, ROI, Rating, Popularity, Runtime). |
| `dim_genres` | Dimension | `genre_id` | Master list of film genres (Action, Drama, Sci-Fi, etc.). |
| `dim_companies` | Dimension | `company_id` | Global production studios and distribution entities. |
| `dim_cast` | Dimension | `actor_id` | On-screen talent registry. |
| `dim_directors` | Dimension | `director_id` | Director directory. |
| `dim_date` | Dimension | `date_key` | Calendar and fiscal hierarchy lookup. |
| `bridge_movie_genres` | Bridge Table | `(movie_id, genre_id)` | Resolves M:N mapping between movies and genres. |
| `bridge_movie_companies`| Bridge Table | `(movie_id, company_id)`| Resolves M:N mapping between movies and studios. |
| `bridge_movie_cast` | Bridge Table | `(movie_id, actor_id)` | Maps actor billing order and character names per movie. |
| `bridge_movie_directors`| Bridge Table | `(movie_id, director_id)`| Maps directing credits per movie. |

---

## 🔒 Data Quality & Governance Gate

To safeguard KPI accuracy against missing or corrupted API records, a **Data Quality Gate** is enforced inside BigQuery's analytical view layer:

```sql
CREATE OR REPLACE VIEW `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` AS
WITH RankedMovies AS (
    SELECT 
        movie_id,
        title,
        original_language,
        release_date,
        budget,
        revenue,
        (revenue - budget) AS profit,
        SAFE_DIVIDE(revenue - budget, budget) * 100 AS roi_percent,
        vote_average,
        vote_count,
        popularity,
        runtime,
        ROW_NUMBER() OVER (
            PARTITION BY movie_id 
            ORDER BY popularity DESC
        ) AS rn
    FROM `tmdb-movie-analytics.tmdb_analytics.fact_movies`
    -- DATA QUALITY GATE: Filter invalid financial noise
    WHERE budget > 0 
      AND revenue > 0
      AND release_date IS NOT NULL
)
SELECT 
    movie_id,
    title,
    original_language,
    release_date,
    EXTRACT(YEAR FROM SAFE_CAST(release_date AS DATE)) AS release_year,
    budget,
    revenue,
    profit,
    roi_percent,
    vote_average,
    vote_count,
    popularity,
    runtime,
    CASE 
        WHEN budget >= 100000000 THEN 'High Budget ($100M+)'
        WHEN budget >= 20000000  THEN 'Mid Budget ($20M-100M)'
        ELSE 'Low Budget (<$20M)'
    END AS budget_category,
    CASE 
        WHEN budget >= 100000000 THEN 3
        WHEN budget >= 20000000  THEN 2
        ELSE 1
    END AS budget_category_sort
FROM RankedMovies
WHERE rn = 1;
```

---

## ⚡ Key Analytical SQL Queries & Verified Outputs

The pipeline repository includes 7 optimized SQL scripts executed against Google BigQuery:

### Query 1: Top Grossing & Most Profitable Directors
```sql
SELECT 
    d.director_name,
    COUNT(DISTINCT f.movie_id) AS total_movies_directed,
    SUM(f.revenue) AS total_revenue,
    SUM(f.profit) AS total_profit,
    ROUND(AVG(f.profit), 2) AS avg_profit_per_movie
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` f
JOIN `tmdb-movie-analytics.tmdb_analytics.bridge_movie_directors` bd ON f.movie_id = bd.movie_id
JOIN `tmdb-movie-analytics.tmdb_analytics.dim_directors` d ON bd.director_id = d.director_id
GROUP BY d.director_name
HAVING COUNT(DISTINCT f.movie_id) >= 3
ORDER BY total_profit DESC
LIMIT 5;
```
*Output Summary:* James Cameron leads globally with **$161.36B** in total profit across 9 releases (averaging **$1.53B** profit per film), followed by Christopher Nolan (**$121.31B**) and Anthony/Joe Russo (**$112.91B**).

### Query 3: Budget Category Performance & Capital Efficiency
```sql
SELECT 
    CASE 
        WHEN budget >= 100000000 THEN 'High Budget ($100M+)'
        WHEN budget >= 20000000  THEN 'Mid Budget ($20M-100M)'
        ELSE 'Low Budget (<$20M)'
    END AS budget_category,
    COUNT(movie_id) AS movie_count,
    SUM(budget) AS category_total_budget,
    SUM(revenue) AS category_total_revenue,
    SUM(profit) AS category_total_profit,
    ROUND((SUM(profit) / SUM(revenue)) * 100, 2) AS profit_margin_percent,
    ROUND(AVG(SAFE_DIVIDE(profit, budget) * 100), 2) AS avg_roi_percent
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique`
GROUP BY budget_category
ORDER BY category_total_budget DESC;
```
*Output Summary:* Mid-Budget productions ($20M–$100M) serve as industry volume backbones ($187.5B profit), whereas High-Budget blockbusters ($100M+) command the strongest margin resiliency (**66.49% Profit Margin**).

---

## 🎨 Power BI Executive Dashboard Interface

The Power BI platform delivers a 3-page interactive decision engine designed according to the **Information Pyramid Principle** (Overview $\rightarrow$ Segment Analysis $\rightarrow$ Granular Ledger).

### Page 1: Executive Overview
* **KPI Header Cards:** Displays Total Movies (`6,655`), Total Revenue (`$707bn`), Total Profit (`$469bn`), Average ROI (`421.9%`), and Average Audience Rating (`6.7`) embedded with context-aware icons.
* **Macro Trends:** Annual Box-Office Revenue Trend Line Chart (2000–2026), highlighting COVID-19 pandemic drawdown (2020) and subsequent structural rebound (2021–2025).
* **Segment Distributions:** Genre Profitability Horizontal Bars and Top Production Studio Revenue Leaderboards.

### Page 2: Talent & Performance Analytics
* **Talent Benchmarks:** Top Grossing Directors & Actors ranked dynamically by total box-office revenues and cumulative profit contributions.
* **Logarithmic Scatter Plot:** Ratings (`Vote Average`) vs. TMDB Popularity Index configured with a logarithmic Y-scale (`0.01` to `10,000.00`) to eliminate visual cluster distortion.
* **Production Categorization:** Revenue breakdown across `Low Budget (<$20M)`, `Mid Budget ($20M-100M)`, and `High Budget ($100M+)`.

### Page 3: Financial Drill-Down Ledger
* **Portfolio Health Donut Chart:** Visually highlights portfolio outcome distribution (**73.36% Profitable** vs **26.64% Loss-making**).
* **Ad-hoc Matrix Ledger:** Drillable financial ledger featuring `Movie Title`, `Release Date`, `Budget ($)`, `Revenue ($)`, `Profit ($)`, `ROI %`, `Vote Average`, `Popularity`, and `Runtime` with conditional color formatting (Green for positive ROI, Red for negative ROI).

---

## 🛠️ Repository Structure

```text
global-movie-analytics-pipeline/
│
├── dags/
│   └── tmdb_incremental_dag.py        # Apache Airflow Daily Incremental ETL DAG
│
├── scripts/
│   └── tmdb_bigquery_etl.py           # Python Ingestion & Processing Pipeline Script
│
├── sql/
│   ├── schema.sql                     # BigQuery Star Schema DDL Script
│   └── analytical_queries.sql         # Top 7 Production Analytical SQL Queries
│
├── power_bi/
│   ├── Global_Movie_Analytics.pbix    # Production Power BI Report File
│   └── screenshots/                   # High-Resolution UI Snapshots
│       ├── page1_executive_overview.png
│       ├── page2_talent_performance.png
│       └── page3_financial_ledger.png
│
├── requirements.txt                   # Python Runtime Dependencies
├── .gitignore                         # Environment Security Rule Set
└── README.md                          # Executive Technical Documentation
```

---

## 👤 Author & Professional Links

**Nihat Rzaguluzada**  
*Information Technologies Student | Data Engineering & Analytics Specialist*  
📍 Baku, Azerbaijan  
* **LinkedIn:** [linkedin.com/in/nihat-rzaguluzada](https://linkedin.com)  
* **GitHub:** [github.com/nihatrza](https://github.com)  

---
*Global Movie Analytics Pipeline is actively deployed on Cloud Infrastructure with daily automated updates.*
