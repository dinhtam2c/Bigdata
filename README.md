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
Producer 
  ↓
Kafka (topic: covid-raw)
  ↓
Consumer 
  ↓
HDFS 
  ↓
Spark Processing
  ↓
Elasticsearch ←→ Kibana UI
```

### 6.2. Luồng dữ liệu
1. **Producer** đọc file CSV và gửi từng bản ghi vào **Kafka** topic `covid-raw`
2. **Consumer** lắng nghe Kafka và ghi dữ liệu vào **HDFS**
3. **Spark** đọc dữ liệu từ HDFS, xử lý và ghi kết quả vào **Elasticsearch**
4. **Kibana** hiển thị Dashboard từ dữ liệu trong Elasticsearch

### 6.3. Batch Mode
Chạy nhanh toàn bộ pipeline trong chế độ Batch:
```bash
# Bước 1: Triển khai infrastructure (chỉ chạy lần đầu)
bash setup.sh

# Bước 2: Gửi dữ liệu vào Kafka
bash run_producer.sh

# Bước 3: Consumer lưu dữ liệu vào HDFS
bash run_consumer.sh

# Bước 4: Spark xử lý và đẩy vào Elasticsearch
bash run_spark_local.sh

# Bước 5: Truy cập Kibana Dashboard
# http://bigdata-server:5601
```

### 6.4. Streaming Mode
Chạy nhanh toàn bộ pipeline trong chế độ Streaming:
```bash
# Bước 1: Triển khai infrastructure (chỉ chạy lần đầu)
bash setup.sh

# Bước 2: Gửi dữ liệu streaming vào Kafka (gửi từng ngày)
bash run_producer.sh streaming

# Bước 3: Consumer lưu dữ liệu vào HDFS
bash run_consumer.sh

# Bước 4: Spark CronJob tự động xử lý mỗi 1 phút
bash run_spark_cronjob.sh

# Bước 5: Truy cập Kibana Dashboard
# http://bigdata-server:5601
```

### 6.5. Các lệnh giám sát hữu ích
```bash
# Xem trạng thái tất cả Pod
kubectl get pods

# Xem log của Producer
kubectl logs -l job-name=covid-producer-job -f

# Xem log của Consumer
kubectl logs -l app=consumer -f

# Xem log của Spark Job
kubectl logs -l job-name=spark-hdfs-to-es -f

# Xem CronJob status
kubectl get cronjob

# Xem HDFS Web UI
# http://bigdata-server:9870

# Xem Spark Web UI
# http://bigdata-server:8080
```

## 7. Vận hành chi tiết

### 7.1. Data Producer
Hệ thống bao gồm một module Producer để đẩy dữ liệu giả lập từ file CSV vào Kafka.

#### Cấu trúc
- **Source Code**: `src/kafka_producer.py` - Script Python đọc CSV và gửi tin nhắn đến Kafka.
- **Dữ liệu**: `data-sources/covid_0.csv` - File dữ liệu nguồn.
- **Manifest**: `k8s-manifests/producer.yaml` - Job Kubernetes chạy Producer.
- **Runner Script**: `run_producer.sh` - Script tự động hóa việc deploy và nạp dữ liệu.

#### Cách chạy
Để bắt đầu quá trình đẩy dữ liệu, chạy lệnh sau:

```bash
bash run_producer.sh
```

Script này sẽ thực hiện các bước:
1. Xóa Job cũ nếu đang chạy.
2. Tạo ConfigMap mới từ code trong `src/kafka_producer.py`.
3. Deploy Job lên Kubernetes.
4. Chờ Pod sẵn sàng.
5. Copy file `data-sources/covid_0.csv` vào Pod để kích hoạt quá trình xử lý.
6. Hiển thị log output.

**Lưu ý:**
- Producer được cấu hình để gửi dữ liệu vào topic `covid-raw`.
- Bạn có thể chỉnh sửa logic gửi tin (tốc độ, format) trong file `src/kafka_producer.py`.

### 7.2. Xử lý hàng loạt (Batch Mode)
Sử dụng khi cần nạp toàn bộ dữ liệu lịch sử và xử lý một lần duy nhất.

#### Bước 1: Gửi dữ liệu vào Kafka
```bash
bash run_producer.sh
```

#### Bước 2: Lưu dữ liệu từ Kafka vào HDFS
```bash
bash run_consumer.sh
```

#### Bước 3: Chạy Spark Job xử lý Batch
```bash
bash run_spark_local.sh
```

Spark sẽ đọc dữ liệu từ HDFS, xử lý và ghi kết quả vào Elasticsearch.

### 7.3. Xử lý luồng giả lập (Streaming Mode)
Dùng để mô phỏng dữ liệu thời gian thực, dữ liệu được gửi theo từng ngày và xử lý liên tục.

#### Bước 1: Gửi dữ liệu Streaming vào Kafka
```bash
bash run_producer.sh streaming
```

#### Bước 2: Khởi động Kafka Consumer ghi dữ liệu vào HDFS
```bash
bash run_consumer.sh
```

#### Bước 3: Tự động hóa Spark Streaming bằng CronJob
```bash
bash run_spark_cronjob.sh
```

Spark CronJob sẽ tự động:

* Quét dữ liệu mới trong HDFS
* Xử lý dữ liệu
* Cập nhật kết quả vào Elasticsearch

**Chu kỳ chạy mặc định: mỗi 2 phút**

## 8. Quản lý tài nguyên và dọn dẹp

### 8.1. Dừng Spark CronJob
```bash
bash stop_spark_cronjob.sh
```

### 8.2. Xóa toàn bộ Kubernetes Cluster (k3d)
```bash
k3d cluster delete bigdata
```

### 8.3. Dọn dẹp tài nguyên Docker dư thừa
```bash
docker system prune -a --volumes
```

