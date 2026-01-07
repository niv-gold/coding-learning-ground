# Copilot Instructions for my_codes

## Project Overview
This is a **learning and experimentation repository** organized into distinct domains:
- **`nyc_taxi/`**: S3 file upload pipeline (AWS integration project)
- **`python/`**: Data pipeline patterns (APIs, ETL, pandas/MySQL workflows)
- **`Spark/`**: Big data processing examples (PySpark, DataFrames, UDFs)
- **`exams/`**: Interview/assessment solutions (Spark SQL, transformations)

**Key Technology Stack**: PySpark, Pandas, SQLAlchemy, AWS S3, REST APIs, MySQL

---

## NYC Taxi S3 Upload Pipeline (`nyc_taxi/`)

### Architecture
- **`config.py`**: Dataclass `S3config` holds AWS credentials, bucket name, local paths, S3 prefix
- **`uploader.py`**: Core orchestration for ingestion files to S3
- **`s3_client.py`**: AWS S3 client wrapper (currently empty, to be implemented)
- **`file_finder.py`**: File discovery logic for local batch processing (stub)
- **`test.py`**: Configuration validation tests

### Data Flow
```
Local Files → file_finder discovers → uploader batches → s3_client uploads → S3 bucket
                                   ↓ configured via config.py ↓
```

### Developer Patterns
1. **Configuration as dataclass**: `S3config` uses `@dataclass` with optional `s3_base_prefix` for folder structure
   - Example: `s3_base_prefix="raw/"` creates `s3://bucket/raw/` directory structure
2. **Tests import from main modules**: `test.py` imports `S3config` directly (not mocked)

### Critical Knowledge
- This is the **active development branch** (`nyc_texi_first_setup`)
- S3 client and file finder modules are **skeleton implementations**—need full implementation
- Configuration is environment-independent; credentials should be externalized (not hardcoded)

---

## Python Data Pipelines (`python/`)

### Subdomain Patterns

#### **API Integration** (`API/REST/`)
- **Pattern**: REST API → JSON/Dict → Pandas DataFrame → MySQL
- **Key Files**: `API_to_MySql_project.py`, `simply_DE_test.py`
- **Workflow**:
  1. `get_response(url, headers, querystring)` → HTTP request with error handling (check `status_code == 200`)
  2. `transform_pd_df(response_dict)` → Convert API response to DataFrame
  3. Filter/clean data → `check_if_movieId_exists()` to avoid duplicates
  4. `.to_sql(con=engine, if_exists='append')` to MySQL
- **Scheduling**: Uses `schedule` library for periodic tasks (`schedule.every().minutes(1).do(main)`)

#### **ETL Operations** (`getting_in_shape/API_ETL.py`)
- **Pattern**: Multi-stage pandas transformations with aggregation and windowing
- **Transformation Pipeline**:
  ```python
  raw_df → increase_death_by_1() → map_as_case_when() → apply_survivers() 
  → self_join() → clean_df_data() → LAG_LEAD_func() → WF_sum_3_rows() 
  → WF_runT_by_state() → group_by_col()
  ```
- **Key Functions**:
  - `LAG_LEAD_func()`: Uses `.shift()` for lag/lead window operations
  - `WF_sum_3_rows()`: Rolling aggregations with `.rolling(window=3).sum()` and `.cumsum()`
  - `apply_survivers()`: Custom row-by-row transformation logic

#### **Pandas Data Processing** (`map/`)
- **Structural Pattern**: Chain functions that each return modified DataFrame
- **File**: `map_on_pandas.py` shows main() pipeline chaining 8+ transformations
- **Functions segment responsibilities**:
  - Data loading: `import_row_data(file_path)` → reads CSV with inferred types
  - Type conversion: `convert_col_to_float()`, `validate_datetime()`
  - Feature engineering: `set_totalSales()`, `set_category()`
  - Aggregation: `total_sales_per_category()`, `product_highest_sales_per_month()`

#### **MySQL Operations** (`read_write_to_db_mysql/`)
- **Connection**: SQLAlchemy `create_engine(f'mysql+pymysql://{user}:{pwd}@{host}/{db}')`
- **Operations**: `df.to_sql()`, raw queries via `text()`, upsert patterns
- **Pattern**: Use `text()` wrapper for raw SQL, inspect tables with `inspect.get_table_names()`

---

## PySpark Processing (`Spark/`)

### Key Patterns

#### **Schema Definition** (Data Type Specification)
- Explicit schemas preferred for production (avoid `inferSchema=True` for large tables)
- Example from `Spark_Data_Types.py`:
  ```python
  schema = StructType([
      StructField("customer_id", DecimalType()),
      StructField("treatment_date", TimestampType())
  ])
  df = spark.read.schema(schema).option("header", "true").load(file)
  ```

#### **DataFrames & Temp Views**
- Create temporary views: `df.createOrReplaceTempView('table_name')`
- Query via SQL: `spark.sql("SELECT * FROM table_name WHERE condition")`
- Managed vs Unmanaged tables: Use `.option('path', output_path)` for unmanaged external tables

#### **Aggregations & Windowing** (`aggregation/`, `perion/code_examples.py`)
- Group-by pattern:
  ```python
  df.groupBy(F.col('Country'), F.col('Year')).agg(
      F.count('*').alias('count'),
      F.sum('Value').alias('sum_values'),
      F.first('Cumulative').alias('first_val')
  ).show()
  ```
- Window functions:
  ```python
  windowSpec = Window.partitionBy('Country').orderBy('Date')\
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
  df.withColumn('sum_over_window', F.sum('Value').over(windowSpec))
  ```

#### **Partitioning Strategy** (`exams/Perion_exam.py`)
- Write partitioned tables: `.write.partitionBy('dt').saveAsTable(name, format='parquet', mode='overwrite')`
- One file per partition pattern:
  ```python
  partition_list = df.select('dt').distinct().rdd.map(lambda row: row['dt']).collect()
  for partition in partition_list:
      df.filter(F.col('dt') == partition).repartition(1).write.mode('append')...
  ```
- Partition modes: `'overwrite'` replaces all data, `'append'` adds incrementally

#### **Exam Patterns** (`exams/`)
- Real-world data processing: Read multiple JSON files, union DataFrames, apply transformations
- Production considerations: Partition for query performance, control file output (1 file/partition)

---

## Critical Code Patterns & Conventions

### Error Handling
1. **API Requests**: Always check `response.status_code == 200` before parsing JSON
   ```python
   if response.status_code != 200:
       raise ValueError(f'Request Error code: {response.status_code}')
   ```
2. **Spark Operations**: Wrap in try-except for table writes/reads

### Data Quality
- **Null handling**: `df.fillna(0)` for filling, `df.drop(subset=['col'])` for removal
- **String operations**: `.str.replace()` for pandas, `F.regexp_replace()` for Spark
- **Type validation**: Cast types explicitly before aggregations

### Testing Pattern
- Tests import production classes/configs directly (e.g., `test.py` imports `S3config`)
- Validation via dataclass fields and print statements
- No formal test framework (pytest/unittest) discovered yet

### Function Naming Conventions
- `get_*`: Fetch/retrieve data (API, DB, files)
- `transform_*`: Reshape data structure
- `*_pd_df` or `pd_df_*`: Indicate pandas DataFrame parameters
- `write_*`: Persist to destination (DB, S3, file)
- `check_*`: Validation/existence checks

---

## Integration Points & External Dependencies

| Component | Purpose | Key Functions |
|-----------|---------|---|
| **SQLAlchemy** | MySQL ORM/SQL execution | `create_engine()`, `text()`, `inspect()` |
| **Pandas** | Data transformation | `.groupby()`, `.apply()`, `.to_sql()` |
| **PySpark** | Distributed processing | `SparkSession`, `DataFrame.write`, `.sql()` |
| **Requests** | HTTP APIs | `.get()`, `.json()`, status code checks |
| **Schedule** | Task scheduling | `.every().minutes(n).do(func)` |
| **AWS SDK (boto3)** | S3 operations | *To be implemented in `s3_client.py`* |

---

## Development Workflows

### Running Python Scripts
```bash
# ETL pipeline
python python/getting_in_shape/API_ETL.py

# REST API → MySQL pipeline
python python/API/REST/simply_DE_test.py

# Pandas transformations
python python/map/map_on_pandas.py
```

### PySpark Jobs
```bash
# Run from CLI (if spark-submit available)
spark-submit Spark/:earning_erea/Learning_Spark_Oreilly/data_frame/DataFrames.py

# Or submit via SparkSession in Python
python exams/Perion_exam.py
```

### NYC Taxi Project Development
- Configure AWS credentials in environment or `.env` file before running
- Implement `s3_client.py` and `file_finder.py` modules
- Validate with `test.py` script

---

## Red Flags & Anti-Patterns to Avoid

1. **Hardcoded Credentials**: Never commit AWS keys; use environment variables or `.env`
2. **`inferSchema=True` on Large Datasets**: Causes significant performance degradation; define schemas explicitly
3. **Unmanaged Spark Table Paths**: Ensure `option('path', ...)` is set for external tables
4. **Missing API Error Checks**: Always validate status code before JSON parsing
5. **No Partition Strategy**: Spark table queries will be slow without partitioning on timestamp/category columns
6. **Module Import Confusion**: `nyc_taxi/test.py` imports from `uploader` directly—ensure module paths are correct when running

---

## Key Files Reference

| File | Purpose | Status |
|------|---------|--------|
| `nyc_taxi/config.py` | S3 configuration dataclass | ✓ Implemented |
| `nyc_taxi/uploader.py` | Main upload orchestration | ⚠️ Skeleton |
| `nyc_taxi/s3_client.py` | AWS S3 wrapper | ⚠️ Empty |
| `python/getting_in_shape/API_ETL.py` | Full ETL pipeline example | ✓ Complete |
| `python/API/REST/simply_DE_test.py` | REST→MySQL workflow | ✓ Complete |
| `exams/Perion_exam.py` | Spark partitioning patterns | ✓ Reference |

