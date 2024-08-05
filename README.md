# README for Airflow DAG: ETL2

## Overview

This Airflow DAG orchestrates an ETL (Extract, Transform, Load) pipeline. It extracts data from CSV files and Elasticsearch, transforms the data, and loads it into MongoDB and CSV files.

## Dependencies

### Python Packages
- `datetime`
- `timedelta` from `datetime`
- `airflow`
- `PythonOperator` from `airflow.operators.python`

### Custom Modules
- `extractor` from `ETL2.Extractor`
- `transporter` from `ETL2.Transporter`
- `loader` from `ETL2.Loader`

## Modules Explanation

### Extractor
Handles data extraction:
- `csv_to_redis`: Extracts data from a CSV file to Redis.
- `elasticsearch_to_redis`: Extracts data from Elasticsearch to Redis.

### Transporter
Handles data transformation:
- `slice_by_row`: Slices data by rows and stores it in Redis.
- `slice_by_column`: Slices data by columns and stores it in Redis.

### Loader
Handles data loading:
- `to_mongo`: Loads data from Redis to MongoDB.
- `to_csv`: Loads data from Redis to a CSV file.

## Default Arguments

Default parameters for the DAG and its tasks:
- `owner`: 'airflow'
- `depends_on_past`: False
- `email_on_failure`: True
- `email_on_retry`: True
- `retries`: 1
- `retry_delay`: 1 minute
- `start_date`: January 1, 2023

## DAG Definition

DAG parameters:
- `dag_id`: 'ETL2'
- `default_args`: `default_args`
- `schedule_interval`: 1 day
- `catchup`: False

## Tasks

1. **CSV Extractor Task**
   - Extracts data from a CSV file to Redis.
   - `task_id`: 'csv_extractor'
   - `python_callable`: `extractor.csv_to_redis`
   - `op_kwargs`: `{'csv_path': '/Users/MeT/Airflow/dags/Data/customers-1000.csv'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

2. **Elasticsearch Extractor Task**
   - Extracts data from Elasticsearch to Redis.
   - `task_id`: 'elasticsearch_extractor'
   - `python_callable`: `extractor.elasticsearch_to_redis`
   - `op_kwargs`: `{'elastic_index': 'customers-1000'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

3. **Row Slice Transform Task**
   - Transforms data by slicing rows and storing it in Redis.
   - `task_id`: 'row_slice_transform'
   - `python_callable`: `transporter.slice_by_row`
   - `op_kwargs`: `{'num': 3, 'redis_dest': 'row_transport_data'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

4. **Column Slice Transform Task**
   - Transforms data by slicing columns and storing it in Redis.
   - `task_id`: 'column_slice_transform'
   - `python_callable`: `transporter.slice_by_column`
   - `op_kwargs`: `{'num': 2, 'redis_dest': 'column_transport_data'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

5. **Row Loader Task**
   - Loads row-sliced data from Redis to MongoDB.
   - `task_id`: 'row_loader_task'
   - `python_callable`: `loader.to_mongo`
   - `op_kwargs`: `{'redis_key': 'row_transport_data'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

6. **Column Loader Task**
   - Loads column-sliced data from Redis to a CSV file.
   - `task_id`: 'column_loader_task'
   - `python_callable`: `loader.to_csv`
   - `op_kwargs`: `{'csv_path': "COLUMN_SLICE", 'redis_key': 'column_transport_data'}`
   - `retries`: 1
   - `retry_delay`: 30 seconds

## Task Dependencies

- Both `csv_extract_task` and `elasticsearch_extract_task` run in parallel.
- `row_slice_transform_task` and `column_slice_transform_task` depend on the completion of both extraction tasks.
- `row_loader_task` depends on `row_slice_transform_task`.
- `column_loader_task` depends on `column_slice_transform_task`.

```python
[csv_extract_task, elasticsearch_extract_task] >> row_slice_transform_task >> row_loader_task
[csv_extract_task, elasticsearch_extract_task] >> column_slice_transform_task >> column_loader_task
```

## Conclusion

This DAG defines a robust ETL pipeline leveraging Airflow's task orchestration capabilities, ensuring automated and reliable data processing.
