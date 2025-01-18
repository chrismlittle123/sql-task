# Bonus Implementation Plan

## 1. Generate Fake Data

### Setup
1. Create a Python script `generate_fake_data.py` using:
   - `faker` library for generating realistic customer names
   - `random` for generating order values and product IDs
   - `pandas` for DataFrame manipulation

### Data Generation
1. Generate 1000 orders with:
   - Random customer names (200 unique customers, eg. Anna Barber, John Smith, etc.)
   - Order IDs (unique integers starting from 100)
   - Prices between 1-100 (float)
   - Order dates (last 6 months, today is 2023-12-10) YYYY-MM-DD
   - Product Names which are one of "transfer", "parking", "fast_track", "lounge"
   - Random passenger counts (1-6)

2. Save as:
   - CSV file for local testing
   - Parquet file for Databricks ingestion

## 2. Databricks ETL Pipeline

### Environment Setup
1. Create Databricks workspace
2. Set up cluster with:
   - Latest Databricks Runtime
   - Auto-termination
   - Appropriate instance type

### Pipeline Development
1. Create notebook `raw_to_normalized.py`:
   ```python
   # Steps:
   1. Read raw parquet file
   2. Create temp view
   3. Apply transformations (reuse SQL from Part 1)
   4. Write normalized tables to Delta format
   ```

2. Create Databricks job:
   - Schedule hourly/daily runs
   - Email notifications for failures
   - Retry logic

### Data Quality
1. Add data quality checks:
   - Primary key uniqueness
   - No null values in required fields
   - Date range validations
   - Value range checks

## 3. Databricks SQL Dashboard

### Metric Queries
1. Total Orders by Customer:
   ```sql
   -- Reuse query from Part 2
   -- Add visualizations:
   - Bar chart of orders by customer
   - Time series of order volume
   ```

2. Most Recent Orders:
   ```sql
   -- Reuse query from Part 2
   -- Add visualizations:
   - Table with latest orders
   - Time since last order by customer
   ```

3. Top Customers Last Week:
   ```sql
   -- Reuse query from Part 2
   -- Add visualizations:
   - Bar chart of customer value
   - Week-over-week comparison
   ```

### Dashboard Layout
1. Create dashboard with:
   - Title and description
   - Date filter (last week/month/year)
   - Refresh schedule (hourly)
   - Three sections for each metric

### Sharing & Access
1. Configure:
   - Public access link
   - Refresh token
   - Download permissions
   - Query timeout settings

## Implementation Timeline
1. Data Generation: 1 day
2. ETL Pipeline: 2 days
3. Dashboard Creation: 1 day
4. Testing & Documentation: 1 day

## Success Criteria
- [ ] Generate 1000+ realistic orders
- [ ] ETL pipeline runs successfully
- [ ] Dashboard updates automatically
- [ ] All metrics match Part 2 results
- [ ] Public access works reliably
