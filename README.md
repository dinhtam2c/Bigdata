# Big Data
Hệ thống xử lý và phân tích dữ liệu COVID-19 thời gian thực, triển khai trên nền tảng Kubernetes (K3d) với kiến trúc Lambda.

## Mục lục
1. [Yêu cầu hệ thống](#1-yêu-cầu-hệ-thống)
2. [Cấu hình mạng](#2-cấu-hình-mạng)
3. [Các cổng dịch vụ](#3-các-cổng-dịch-vụ)
4. [Triển khai hệ thống](#4-triển-khai-hệ-thống)
5. [Kiểm tra hệ thống](#5-kiểm-tra-hệ-thống)
6. [Workflow hoàn chỉnh](#6-workflow-hoàn-chỉnh)
7. [Vận hành chi tiết](#7-vận-hành-chi-tiết)
8. [Quản lý tài nguyên và dọn dẹp](#8-quản-lý-tài-nguyên-và-dọn-dẹp)

## 1. Yêu cầu hệ thống

### 1.1. Phần mềm
1. Docker >= v20.10.5 (runc >= v1.0.0-rc93)
2. k3d 5.8.3
3. kubectl

### 1.2. Phần cứng
Hệ thống cần tối thiểu **~20 GB dung lượng trống** để khởi tạo Persistent Volumes:

| Thành phần       | Dung lượng |
| ---------------- | ---------- |
| Elasticsearch    | ~5 GB      |
| HDFS DataNode    | ~10 GB     |
| Kafka + NameNode | ~4 GB      |

## 2. Cấu hình mạng
Cần cấu hình file host
- **Linux/Mac:** `/etc/hosts`
- **Windows:** `C:\Windows\System32\drivers\etc\hosts`

Thêm dòng: `<IP server>  bigdata-server`

Ví dụ: `127.0.0.1  bigdata-server`

## 3. Các cổng dịch vụ
| Dịch vụ       | Cổng Host | Cổng NodePort | Vai trò                        |
| ------------- | --------- | ------------- | ------------------------------ |
| Kafka Broker  | 9092      | 30092         | Nơi đẩy dữ liệu COVID thô      |
| HDFS RPC      | 8020      | 30020         | Cổng đọc/ghi dữ liệu của Spark |
| HDFS Web UI   | 9870      | 30870         | Xem tình trạng file hệ thống   |
| Spark Master  | 7077      | 30077         | Cổng Submit Job xử lý          |
| Spark Web UI  | 8080      | 30080         | Theo dõi tiến độ tính toán     |
| Elasticsearch | 9200      | 32000         | Lưu kết quả phân tích          |
| Kibana UI     | 5601      | 32601         | Giao diện Dashboard            |

## 4. Triển khai hệ thống

### 4.1. Cấu hình bộ nhớ ảo
Elasticsearch yêu cầu bộ nhớ ảo cao hơn mức mặc định.
Kiểm tra output của `sysctl vm.max_map_count`. Nếu < 262144 thì chạy lệnh.
```bash
sudo sysctl -w vm.max_map_count=262144
```
Lệnh chỉ có hiệu ứng tạm thời, sẽ mất khi khởi động lại.

### 4.2. Tạo cluster
Có thể cần cấu hình mạng cho docker để có thể pull image.

Sau đó chạy để tạo cluster
```bash
bash setup.sh
```

## 5. Kiểm tra hệ thống
1. Kiểm tra trạng thái các Pod: `kubectl get pods`
2. Kiểm tra kết nối tới Kafka từ xa `ncat -zv bigdata-server 9092`
3. Kiểm tra API Elasticsearch `curl http://bigdata-server:9200`

## 6. Workflow hoàn chỉnh

### 6.1. Sơ đồ kiến trúc
```
                    CSV Data (covid_0.csv)
                            ↓
                Producer Streaming (gửi theo ngày)
                            ↓
                    Kafka (topic: covid-raw)
                            ↓
            ┌───────────────┴───────────────┐
            ↓                               ↓
        Consumer                    Spark Stateful Streaming
            ↓                         (xử lý real-time)
          HDFS                              ↓
            ↓                               ↓
    Spark CronJob                           ↓
  (batch processing)                        ↓
            ↓                               ↓
            └───────────────┬───────────────┘
                            ↓
                    Elasticsearch
                            ↓
                       Kibana UI
```

### 6.2. Luồng dữ liệu
Hệ thống có 2 luồng xử lý song song:

**Luồng Batch (qua HDFS):**
1. **Producer Streaming** đọc file CSV và gửi dữ liệu theo từng ngày vào **Kafka** topic `covid-raw`
2. **Consumer** lắng nghe Kafka và ghi dữ liệu vào **HDFS**
3. **Spark CronJob** định kỳ xử lý dữ liệu từ HDFS và ghi vào **Elasticsearch**

**Luồng Real-time (trực tiếp từ Kafka):**
1. **Producer Streaming** gửi dữ liệu vào **Kafka** topic `covid-raw`
2. **Spark Stateful Streaming** đọc trực tiếp từ Kafka, xử lý real-time và ghi vào **Elasticsearch**

**Visualization:**
- **Kibana** hiển thị Dashboard từ dữ liệu trong Elasticsearch (cả batch và real-time)

### 6.3. Các bước triển khai
```bash
# Bước 1: Triển khai infrastructure (chỉ chạy lần đầu)
bash setup.sh

# Bước 2: Khởi động Spark CronJob (chạy định kỳ mỗi 10 phút)
bash run_spark_cronjob.sh

# Bước 3: Khởi động Spark Stateful Streaming (xử lý real-time)
bash run_spark_stateful_streaming.sh

# Bước 4: Gửi dữ liệu streaming vào Kafka (gửi từng ngày)
bash run_producer.sh streaming

# Bước 5: Truy cập Kibana Dashboard
# http://bigdata-server:5601
```

### 6.4. Các lệnh giám sát hữu ích
```bash
# Xem trạng thái tất cả Pod
kubectl get pods

# Xem log của Consumer
kubectl logs -l app=consumer -f

# Xem log của Spark CronJob
kubectl get cronjob
kubectl logs -l job-name=spark-hdfs-to-es-cron --tail=50 -f

# Xem log của Spark Stateful Streaming
kubectl logs -l app=spark-stateful-streaming -f

# Xem log của Producer Streaming
kubectl logs -l app=covid-producer-streaming -f

# Xem HDFS Web UI
# http://bigdata-server:9870

# Xem Spark Web UI
# http://bigdata-server:8080

# Dừng Spark CronJob
bash stop_spark_cronjob.sh
```

## 7. Vận hành chi tiết

### 7.1. Consumer
Kafka Consumer lắng nghe topic `covid-raw` và ghi dữ liệu vào HDFS.

#### Cấu trúc
- **Source Code**: `src/kafka_consumer.py` - Script Python đọc dữ liệu từ Kafka và ghi vào HDFS.
- **Manifest**: `k8s-manifests/consumer.yaml` - Deployment Kubernetes chạy Consumer.
- **Runner Script**: `run_consumer.sh` - Script tự động hóa việc deploy Consumer.

#### Cách chạy
```bash
bash run_consumer.sh
```

### 7.2. Spark CronJob
Spark CronJob định kỳ đọc dữ liệu từ HDFS, xử lý và ghi vào Elasticsearch.

#### Cấu trúc
- **Source Code**: `src/spark_hdfs_to_elasticsearch.py` - Script Spark xử lý batch từ HDFS.
- **Manifest**: `k8s-manifests/spark-cronjob.yaml` - CronJob Kubernetes.
- **Runner Script**: `run_spark_cronjob.sh` - Script deploy CronJob.

#### Cách chạy
```bash
bash run_spark_cronjob.sh
```

**Chu kỳ chạy mặc định: mỗi 10 phút**

Spark CronJob sẽ tự động:
* Quét dữ liệu mới trong HDFS
* Xử lý dữ liệu
* Cập nhật kết quả vào Elasticsearch

### 7.3. Spark Stateful Streaming
Spark Stateful Streaming xử lý dữ liệu real-time trực tiếp từ Kafka.

#### Cấu trúc
- **Source Code**: `src/spark_kafka_stateful_realtime.py` - Script Spark Streaming xử lý real-time.
- **Manifest**: `k8s-manifests/spark-stateful-streaming.yaml` - Deployment Kubernetes.
- **Runner Script**: `run_spark_stateful_streaming.sh` - Script deploy Streaming.

#### Cách chạy
```bash
bash run_spark_stateful_streaming.sh
```

### 7.4. Producer Streaming
Producer Streaming đọc file CSV và gửi dữ liệu theo từng ngày vào Kafka để mô phỏng dữ liệu thời gian thực.

#### Cấu trúc
- **Source Code**: `src/kafka_producer_streaming.py` - Script Python gửi dữ liệu theo ngày.
- **Dữ liệu**: `data-sources/covid_0.csv` - File dữ liệu nguồn.
- **Manifest**: `k8s-manifests/producer-streaming.yaml` - Deployment Kubernetes.
- **Runner Script**: `run_producer.sh` - Script deploy Producer.

#### Cách chạy
```bash
bash run_producer.sh streaming
```

Script này sẽ thực hiện các bước:
1. Xóa Job cũ nếu đang chạy.
2. Tạo ConfigMap mới từ code trong `src/kafka_producer_streaming.py`.
3. Deploy Job lên Kubernetes.
4. Chờ Pod sẵn sàng.
5. Copy file `data-sources/covid_0.csv` vào Pod.
6. Hiển thị log output.

**Lưu ý:**
- Producer được cấu hình để gửi dữ liệu vào topic `covid-raw`.
- Dữ liệu được gửi theo từng ngày để mô phỏng streaming thực tế.

## 8. Quản lý tài nguyên và dọn dẹp

### 8.1. Xóa dữ liệu Kafka + HDFS + Elasticsearch
Nếu chỉ muốn xóa dữ liệu mà giữ nguyên hệ thống:
```bash
bash clear_data.sh
```

### 8.2. Dừng Spark CronJob
```bash
bash stop_spark_cronjob.sh
```

### 8.3. Xóa toàn bộ Kubernetes Cluster (k3d)
```bash
k3d cluster delete bigdata
```

### 8.4. Dọn dẹp tài nguyên Docker dư thừa
```bash
docker system prune -a --volumes
```
