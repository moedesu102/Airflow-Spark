# E-commerce Data Pipeline (Airflow + Spark)

Pipeline ETL xử lý dữ liệu e-commerce theo kiến trúc **Medallion** (Bronze → Silver → Gold) sử dụng Apache Airflow và Apache Spark.

---

## 1) Yêu cầu hệ thống

- **Docker Desktop** (Windows/Mac) hoặc Docker Engine (Linux)
- **Docker Compose** (có sẵn trong Docker Desktop)
- **RAM**: >= 8GB (tối thiểu 4GB)
- **CPU**: >= 2 cores

---

## 2) Kiến trúc hệ thống

| Service | Mô tả |
|---------|-------|
| **Airflow** | Điều phối workflow (DAG) |
| **Postgres** | Database chứa dữ liệu nguồn + metadata Airflow |
| **Redis** | Message broker cho CeleryExecutor |
| **Spark** | Xử lý batch job qua SparkSubmitOperator |

---

## 3) Cấu trúc thư mục

```
airflow-docker/
├── dags/
│   ├── generate_data.py          # Tạo schema + seed dữ liệu ecommerce
│   ├── load_dag.py               # DAG ETL pipeline chính
│   └── reset_ecommerce_schema.py # Reset toàn bộ schema ecommerce
│
├── spark_scripts/
│   ├── bronze/
│   │   ├── run_bronze.py         # Postgres → Bronze (incremental với watermark)
│   │   └── stream_clickstream.py # Ingest clickstream CSV → Bronze
│   ├── silver/
│   │   └── run_silver.py         # Bronze → Silver (clean + deduplicate)
│   ├── gold/
│   │   └── run_gold.py           # Silver → Gold (dim/fact + analytics)
│   └── generate_clickstream_csv.py # Tạo file CSV clickstream mẫu
│
└── data/                         # Output parquet (mount: /opt/spark-data)
    ├── bronze/
    ├── silver/
    └── gold/
```

---

## 4) Mô tả chi tiết các file

### DAGs (`dags/`)

| File | DAG ID | Chức năng |
|------|--------|-----------|
| `generate_data.py` | `generate_data` | Tạo 11 bảng ecommerce + seed dữ liệu mẫu + tạo clickstream CSV |
| `load_dag.py` | `ecommerce_etl_pipeline` | Pipeline ETL: `stream_clickstream → batch_bronze → batch_silver → batch_gold` |
| `reset_ecommerce_schema.py` | `reset_ecommerce_schema` | Drop toàn bộ tables để reset môi trường |

### Spark Scripts (`spark_scripts/`)

| File | Chức năng |
|------|-----------|
| `bronze/run_bronze.py` | Đọc incremental từ Postgres (dựa trên `updated_at`) → ghi Parquet Bronze |
| `bronze/stream_clickstream.py` | Ingest clickstream từ CSV (batch/streaming mode) → Bronze |
| `silver/run_silver.py` | Làm sạch dữ liệu (trim, validate email/phone, deduplicate theo PK) |
| `gold/run_gold.py` | Tạo dimension/fact tables + 5 bảng analytics (Q1-Q5) |
| `generate_clickstream_csv.py` | Sinh file `clickstream_sample.csv` với dữ liệu mẫu |

### Gold Layer Analytics

| Table | Mô tả |
|-------|-------|
| `dim_users`, `dim_products`, `dim_promotions`, `dim_marketing_channels` | Dimension tables |
| `fact_orders`, `fact_order_items` | Fact tables |
| `gold_q1_conversion_funnel` | Conversion funnel theo category (View → Cart → Purchase) |
| `gold_q2_rfm_segmentation` | Phân khúc khách hàng theo RFM (VIP, Loyal, Churn Risk...) |
| `gold_q3_promo_comparison`, `gold_q3_promo_detail` | Hiệu quả khuyến mãi |
| `gold_q4_inventory_analysis` | Phân tích tồn kho (DSI, slow-moving, low stock) |
| `gold_q5_attribution` | Marketing attribution (First-touch, Last-touch, Linear) |

---

## 5) Khởi động hệ thống

```bash
cd airflow-docker
docker compose up -d --build
```

Chờ 1-3 phút để `airflow-init` hoàn tất.

---

## 6) Truy cập Airflow UI

- **URL**: http://localhost:8080
- **Username**: `airflow`
- **Password**: `airflow`

---

## 7) Hướng dẫn chạy

### Lần đầu tiên

1. **Trigger `generate_data`** - Tạo schema + seed dữ liệu
2. **Trigger `ecommerce_etl_pipeline`** - Chạy ETL pipeline

### Những lần sau

- Chỉ cần trigger `ecommerce_etl_pipeline`

### Reset môi trường

- Trigger `reset_ecommerce_schema` để xóa toàn bộ tables

---

## 8) Output

Sau khi chạy xong, dữ liệu được lưu tại:

| Layer | Đường dẫn | Format |
|-------|-----------|--------|
| Bronze | `./data/bronze/<table>/` | Parquet |
| Silver | `./data/silver/<table>/` | Parquet (partitioned by `_ingestion_date`) |
| Gold | `./data/gold/<table>/` | Parquet |

---

## 9) Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| Không thấy DAG | Chờ 1-2 phút hoặc `docker compose restart` |
| Spark job fail | Kiểm tra Spark master (`spark://spark-master:7077`) và log trong Airflow UI |
| Lỗi connection Postgres | Kiểm tra service postgres đang chạy: `docker compose ps` |
