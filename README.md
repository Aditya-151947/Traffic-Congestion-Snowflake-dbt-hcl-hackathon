# Traffic Congestion & Air Pollution Analysis (Snowflake + dbt)

## Overview
This project is built for **HCL Hackathon evaluation**.  
It analyzes **traffic congestion and air pollution** data using **Snowflake** and **dbt** with a **Medallion Architecture**.

---

## Architecture
**Bronze → Silver → Gold**

- **Bronze**: Raw JSON traffic sensor data  
- **Silver**: Cleaned, deduplicated data  
- **Gold**: Business-level aggregated insights

---

## Tech Stack
- Snowflake
- dbt
- SQL
- GitHub

---

## Key Features
- JSON parsing using `LATERAL FLATTEN`
- Deduplication using `ROW_NUMBER()`
- Window functions (`LAG`, `COUNT OVER`)
- Gridlock detection
- Hourly traffic health summary
- dbt tests using `schema.yml`

---

## Project Structure
models/
├─ silver/
├─ gold/
analysis/
├─ sensor_delta.sql
├─ gridlock_detection.sql



## Group members are: 
- Aditya Yadav
- Aniket Sarve
- Aditi Agre
- Aachal Kothe
