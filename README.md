# Email Marketing Performance & User Retention Pipeline

## Project Overview
This project presents an advanced SQL analytical pipeline developed in BigQuery. It harmonizes user registration data with multi-channel email engagement logs to identify high-value markets and monitor retention trends.

## Technical Complexity & Skills Demonstrated
- **Multi-Stage ETL Logic:** Structured using 7 Common Table Expressions (CTEs) to transform raw logs into a clean, aggregated reporting layer.
- **Advanced Data Merging:** Utilized the `UNION ALL` and `SUM` pattern to synchronize datasets with different levels of granularity (account creation vs. daily email events).
- **Analytical Window Functions:** - `SUM(...) OVER(PARTITION BY...)` to calculate global country-level benchmarks.
  - `DENSE_RANK()` to dynamically identify Top-10 performing markets based on volume and engagement.
- **Data Transformation:** Managed complex date arithmetic (`DATE_ADD`) and conditional logic (`CASE`, `IF`) for data cleaning and normalization.

## Key Business Insights
The resulting dataset allows for:
1. **Market Prioritization:** Automatically identifies Top-10 countries by user acquisition and email activity.
2. **Engagement Tracking:** Monitors the "Open Rate" and "Visit Rate" relative to the account verification status.
3. **Retention Analysis:** Tracks how email activity evolves relative to the user's registration date.

## How to Use
The script is optimized for BigQuery and is designed to feed BI dashboards (Looker, Tableau, Power BI) for executive-level reporting.
