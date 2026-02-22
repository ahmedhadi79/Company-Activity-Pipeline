# Company-Activity-Pipeline

## Target analytics table: 
    `fact_company_activity_daily`

## Grain:  
    `one row per company per day`

This support:
* Daily dashboarding
* Time series analysis
* Rolling metrics (7days, 30days)
* churn detection

## Columns
| Column      | Type     | Description |
|:------------|:--------:|------------:|
| company_id  | STRING   | Company Identifier |
| name   | STRING | From CRM |
| country | STRING | From CRM |
| industry_tag  | STRING   | From CRM          |
| last_contact_at   | TIMESTAMP | From CRM |
| activity_date | DATE | Activity Date |
| active_users  | INT   | From API          |
| events   | INT | From API |
| events_per_user | FLOAT | Drived Metric |
| rolling_7d_active_users   | INT | Day sum-7 |
| is_churn_risk   | BOOLEAN | Drived Flag |

## Sample SQL
Let's assume there are two tables:
* stg_crm_company_daily
* stg_product_usage_daily

```sql
WITH base AS (
    SELECT
    u.data AS activity_date,
    u.company_id,
    c.name AS company_name, 
    c.country, 
    c.industry_tag, 
    c.last_contact_at,
    u.active_users,
    u.events,
    CASE 
            WHEN u.active_users > 0 
            THEN u.events * 1.0 / u.active_users
            ELSE 0 
        END AS events_per_user

    FROM stg_product_usage_daily u
    LEFT JOIN stg_crm_company_daily c
        ON u.company_id = c.company_id
),
rolling_metrics AS (

    SELECT
        *,
        SUM(active_users) OVER (
            PARTITION BY company_id
            ORDER BY activity_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_active_users
    FROM base
)
SELECT
    *,
    CASE 
        WHEN rolling_7d_active_users < 5 
             AND activity_date >= CURRENT_DATE - 7
        THEN TRUE
        ELSE FALSE
    END AS is_churn_risk
FROM rolling_metrics;
```
## Azure Data Factory Flow
```mermaid
[CRM CSV in Blob]
        ↓
Copy Activity → stg_crm_company_daily
        ↓
------------------------------------------------

[ADF Pipeline]
        ↓
Web Activity (Call Product API)
        ↓
Azure Function / Python
        ↓
Write JSON to Blob (raw layer)
        ↓
Copy Activity → stg_product_usage_daily
        ↓
SQL Stored Procedure → Load fact_company_activity_daily
        ↓
Success / Failure Alerts
```
## Python Pseudocode for API Ingestion
```python
import requests
import json
from datetime import datetime, timedelta

def fetch_product_usage(start_date, end_date):
    current_date = start_date
    
    while current_date <= end_date:
        
        response = requests.get(
            "https://api.product.com/usage",
            params={"date": current_date.strftime("%Y-%m-%d")},
            headers={"Authorization": "Bearer <API_TOKEN>"}
        )
        
        if response.status_code != 200:
            raise Exception(f"API failed for {current_date}")
        
        data = response.json()
        
        # Write to Blob (raw layer)
        file_name = f"raw/product_usage/{current_date}.json"
        
        upload_to_blob(
            container="data-lake",
            file_name=file_name,
            data=json.dumps(data)
        )
        
        current_date += timedelta(days=1)


def upload_to_blob(container, file_name, data):
    # Azure Blob SDK logic here
    pass
```
## 30 minutes before tomorrow’s run
I would implement first:

The raw ingestion from API → Blob (with idempotency)

Why?

If raw data isn’t captured, everything downstream fails.

Raw layer is foundational.

You can always backfill transformations later.

What I would postpone:

Advanced rolling metrics

Churn logic tuning

Optimised indexing

Fancy error handling