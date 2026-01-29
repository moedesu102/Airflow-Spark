# Sử dụng image chính thức của Airflow
FROM apache/airflow:3.1.6-python3.10

# Cài đặt các dependencies bổ sung nếu cần (ví dụ, các thư viện hệ thống)
USER root

# Cập nhật và cài đặt OpenJDK-17, Ant
RUN apt update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean;

# Đặt JAVA_HOME cho toàn bộ container
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# TẠO SYMLINK ĐỂ ĐỒNG BỘ ĐƯỜNG DẪN VỚI SPARK Master/Worker
RUN ln -sf /usr/local/bin/python3 /usr/bin/python3

# THIẾT LẬP ĐƯỜNG DẪN CHUNG CHO CẢ DRIVER VÀ SPARK Master/Worker
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Chuyển về user airflow để chạy các lệnh Airflow
USER airflow

# Copy file requirements.txt vào container
COPY requirements.txt .

# Cài đặt các dependencies trong requirements.txt
RUN pip install -r requirements.txt