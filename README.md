# Data Warehousing and Analytics Project

This project demonstrates an end-to-end **data warehousing** and **analytics pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)**. Raw operational data is **ingested, cleaned, standardized, modeled, and aggregated** to support high-quality analytical insights.

The work includes extensive **data validation**, **anomaly handling**, **data enrichment**, and **automated ETL orchestration** through stored procedures.

<img width="995" height="557" alt="截屏2025-11-14 下午1 14 54" src="https://github.com/user-attachments/assets/30ddfd38-7c99-464a-95d7-4730fc4da2a2" />
---

## Architecture Overview

### **Bronze Layer — Raw Operational Data**

* **Full-load batch ingestion**
* **Truncate-and-insert workflow**
* Stores **unmodified source data**
* Preserves **lineage** for audits and debugging

---

## Silver Layer — Cleaned & Transformed Data

This layer performs detailed **data quality processing**, including:

### **Customer Data (CRM)**

* **Deduplication** using `ROW_NUMBER()`
* **Whitespace trimming**
* **Gender normalization**
* **Marital status standardization**
* **Invalid ID removal**
* Added **warehouse metadata columns**

### **Product Data**

* **Normalization** of product line codes
* **Standardized category IDs** (cleaned `cat_id`)
* **Key splitting and correction**
* **Window functions** to repair invalid dates
* Fixing **NULL or negative numeric values**

### **Sales Details**

* **Date format conversion** and repair
* Handling **invalid or out-of-range** dates
* Fixing **negative or inconsistent sales/price** values
* **Recomputed sales measures**
* Ensured **temporal consistency** (order < ship < due)

### **ERP Tables (Customer / Location / Category)**

* Removing **unprintable characters**
* **Standardizing country names**
* **Gender normalization across systems**
* Cleaning **mixed-format IDs and prefixes**
* Standardizing **maintenance categories**

---

## Stored Procedure Automation

A full Silver-layer ETL is automated using:

```
silver.load_silver
```

The procedure performs:

* **Automated truncation + reload**
* **Cleaning and transformation** steps
* Recomputation of **derived columns**
* **Execution-time logging**
* **Error handling** and reporting

This enables a **production-grade ETL pipeline**.

---

## Gold Layer — Modeled Analytics Data

The Gold layer integrates Silver tables to create:

* **Dimension tables** (Customer, Product, Date, Location, Category)
* **Fact tables** (Sales and Orders)
* **Analytical views** for BI tools
* **Aggregated metrics** for reporting

This layer provides optimized structures for **analytics**, **dashboards**, and **KPI measurement**.

---

## ETL Pipeline Summary

### **1. Extract**

Load raw CRM/ERP data into Bronze.

### **2. Transform**

* **Deduplicate, clean, normalize, enrich**
* Fix **structural inconsistencies**
* **Validate and repair** dates, prices, and keys
* Apply **window functions** and **conditional logic**
* Standardize **categorical fields**

### **3. Load**

Deliver trusted Silver data and build **Gold fact/dimension models**.

---

## Analytics & Reporting

The Gold layer enables:

* **Customer segmentation**
* **Product lifecycle analysis**
* **Sales trend analytics**
* **Order behavior insights**
* **Dashboard-ready BI datasets**

---

## Summary

This project demonstrates practical, production-ready **data engineering** and **analytics** skills, including:

* **Complex SQL transformations**
* **Data cleaning and standardization**
* **Deduplication and anomaly correction**
* **Fact/dimension modeling**
* **Medallion architecture implementation**
* **ETL automation using stored procedures**
* Preparing **analytics-ready datasets**

It provides a complete example of building a **modern, scalable data warehouse** from end to end.

https://public.tableau.com/views/databaseproject/Dashboard1?:language=zh-CN&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link
