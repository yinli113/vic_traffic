# 🚦 Traffic Data Pipeline with Medallion Architecture

This repository contains a PySpark-based data pipeline for analyzing traffic sensor data using the **Medallion Architecture** (Bronze, Silver, Gold) in Databricks. It transforms raw CSV traffic data into clean, aggregated, and analytics-ready tables optimized for Power BI dashboards.



---

## 🔥 Pipeline Overview

### 1️⃣ Bronze Layer
- **Purpose:** Ingest raw traffic sensor data from CSV.
- **Key Table:** `raw_traffic_<date>`
- **File:** `build_bronze.py`

### 2️⃣ Silver Layer
- **Purpose:** Clean and transform raw data into a normalized star schema.
- **Key Tables:**
  - `traffic_silver_fact`
  - `dim_region`
  - `dim_site`
  - `dim_time`
  - `dim_detector`
- **File:** `build_silver.py`

### 3️⃣ Gold Layer
- **Purpose:** Create aggregated, analytics-ready tables for dashboards.
- **Key Tables:**
  - `traffic_gold_region_hourly`
  - `traffic_gold_detector_hourly`
  - `traffic_gold_region_monthly`
  - `traffic_gold_congestion_flags`
- **File:** `build_gold.py`

---

## 🧪 Testing

- **Bronze Layer:** `build_bronze_test.py`
  - Validates row count and region count.
- **Silver Layer:** `build_silver_test.py`
  - Validates table creation and data integrity.
- **Gold Layer:** `build_gold_test.py`
  - Validates that Gold tables exist, contain data, and include expected columns.

---

## 📊 Business Results

Below are some key **Power BI visualizations** and **insights** derived from the Gold tables:

### 🔹 Hourly Traffic Volume by Region
![Hourly Traffic Volume by Region](./path/to/Screenshot-2025-06-09-122100.png)

- Peak traffic hours identified around **16:00 - 17:00** across all regions.
- Notable congestion observed in regions **GE2, GR2, VI2**.

---

### 🔹 Monthly Region-Level Volume
![Monthly Region-Level Volume](./path/to/Screenshot-2025-06-09-141558.png)

- Clear monthly volume trends, showing seasonal variations and growth patterns.

---

### 🔹 Detector-Level Analysis
![Detector-Level Volume](./path/to/Screenshot-2025-06-09-142215.png)

- Detector 2 consistently has the highest hourly volume, indicating potential bottlenecks.

---

### 🔹 Congestion Heatmaps and Alerts
![Congestion Alerts by Time](./path/to/Screenshot-2025-06-09-141830.png)

- Congestion peaks typically at **17:15** and **16:45**, aligning with end-of-day traffic surges.

---

## 🚦 Gold Layer Details

| Table Name                      | Description                                    |
|---------------------------------|------------------------------------------------|
| `traffic_gold_region_hourly`    | Hourly traffic volume aggregated by region.    |
| `traffic_gold_detector_hourly`  | Hourly traffic volume at each detector (lane). |
| `traffic_gold_region_monthly`   | Monthly aggregated traffic volume by region.   |
| `traffic_gold_congestion_flags` | Congestion detection flags with thresholds.    |

---

## 📊 Power BI Integration

- Gold tables feed Power BI dashboards for:
  - Real-time traffic volume monitoring.
  - Lane-level congestion analysis.
  - Time-of-day and region-based traffic insights.

---

## 🛣️ Next Steps

- Enhance the `dim_detector` table with lane type and movement direction metadata.
- Integrate real-time streaming ingestion for Bronze ➡️ Silver ➡️ Gold pipelines.
- Optimize query performance with partitioning and Z-Ordering.

---

## 🚀 Contact

For questions or enhancements, please reach out to Yin Li at yin.li.aus@gmail.com.



---



