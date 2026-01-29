# GUIDE: Cài môi trường & chạy DAG (Airflow + Spark)

## 1) Yêu cầu trước khi chạy

- Docker Desktop (Windows/Mac) hoặc Docker Engine (Linux)
- Docker Compose (thường có sẵn trong Docker Desktop)
- Khuyến nghị:
  - RAM >= 8GB (tối thiểu 4GB)
  - CPU >= 2 cores

## 2) Các thành phần trong project

- Airflow: điều phối workflow (DAG)
- Postgres: database chứa dữ liệu nguồn (và metadata Airflow)
- Redis: broker cho CeleryExecutor
- Spark: chạy các batch job được gọi từ Airflow qua SparkSubmit

Các thư mục quan trọng:

- `dags/`
  - `generate_data.py`: tạo schema + seed dữ liệu ecommerce vào Postgres
  - `load_dag.py`: pipeline Spark `batch_bronze >> batch_silver >> batch_gold`
- `spark_scripts/`
  - `bronze/run_bronze.py`: đọc Postgres -> ghi parquet bronze (và update watermark `updated_at`)
  - `silver/run_silver.py`: bronze -> silver (clean + dedup)
  - `gold/run_gold.py`: silver -> gold (dim/fact + Q1..Q5)
- `data/`: nơi lưu parquet output (được mount vào container là `/opt/spark-data`)

## 3) Khởi động hệ thống

Mở terminal tại thư mục `airflow-docker` và chạy:

```bash
docker compose up -d --build
```

Chờ 1-3 phút để `airflow-init` chạy xong và các service ổn định.

## 4) Mở Airflow UI

- URL: `http://localhost:8080`
- Tài khoản mặc định (theo `docker-compose.yaml`):
  - Username: `airflow`
  - Password: `airflow`

## 5) Cách chạy DAG tạo dữ liệu: `generate_data`

Mục tiêu: tạo các bảng ecommerce + seed dữ liệu vào Postgres.

Trong Airflow UI:

1. Vào tab **DAGs**
2. Tìm DAG: `generate_data`
3. Bấm **Unpause**
4. Bấm **Trigger DAG** (nút Play)

Chạy xong DAG này thì Postgres đã có dữ liệu để ETL dùng.

## 6) Cách chạy DAG ETL: `ecommerce_etl_pipeline`

DAG nằm trong `dags/load_dag.py`.

Pipeline hiển thị rõ 3 batch:

```
batch_bronze >> batch_silver >> batch_gold
```

Trong Airflow UI:

1. Tìm DAG: `ecommerce_etl_pipeline`
2. Bấm **Unpause**
3. Bấm **Trigger DAG**

## 7) Output được ghi ở đâu?

Sau khi chạy xong:

- Bronze: `./data/bronze/<table>/...`
- Silver: `./data/silver/<table>/...`
- Gold: `./data/gold/<table>/...`

## 8) Thứ tự chạy khuyến nghị (dễ nhớ)

- Lần đầu:
  1. Trigger `generate_data`
  2. Trigger `ecommerce_etl_pipeline`

- Những lần sau (chỉ ETL):
  - Trigger lại `ecommerce_etl_pipeline`

## 9) Troubleshooting nhanh

- Không thấy DAG:
  - Chờ 1-2 phút để dag-processor load DAG
  - Hoặc restart stack: `docker compose restart`

- Job Spark fail:
  - Kiểm tra Spark master có chạy và Airflow có connect được `spark://spark-master:7077`
  - Kiểm tra log của task trong Airflow UI
