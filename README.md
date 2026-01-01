# Big Data
Hệ thống xử lý và phân tích dữ liệu COVID-19 thời gian thực, triển khai trên nền tảng Kubernetes (K3d) với kiến trúc Lambda.

## 1. Cấu hình mạng
Cần cấu hình file host
- **Linux/Mac:** `/etc/hosts`
- **Windows:** `C:\Windows\System32\drivers\etc\hosts`

Thêm dòng: `<IP server>  bigdata-server`

Ví dụ: `10.69.69.1  bigdata-server`


## 2. Các cổng dịch vụ
| Dịch vụ       | Cổng Host | Cổng NodePort | Vai trò                        |
| ------------- | --------- | ------------- | ------------------------------ |
| Kafka Broker  | 9092      | 30092         | Nơi đẩy dữ liệu COVID thô      |
| HDFS RPC      | 8020      | 30020         | Cổng đọc/ghi dữ liệu của Spark |
| HDFS Web UI   | 9870      | 30870         | Xem tình trạng file hệ thống   |
| Spark Master  | 7077      | 30077         | Cổng Submit Job xử lý          |
| Spark Web UI  | 8080      | 30080         | Theo dõi tiến độ tính toán     |
| Elasticsearch | 9200      | 32000         | Lưu kết quả phân tích          |
| Kibana UI     | 5601      | 32601         | Giao diện Dashboard            |

## 3. Cách tự triển khai
### 3.1. Yêu cầu
1. Docker >= v20.10.5 (runc >= v1.0.0-rc93)
2. k3d 5.8.3
3. kubectl

### 3.2. Triển khai
Elasticsearch yêu cầu bộ nhớ ảo cao hơn mức mặc định.
Kiểm tra output của `sysctl vm.max_map_count`. Nếu < 262144 thì chạy lệnh.
```bash
sudo sysctl -w vm.max_map_count=262144
```
Lệnh chỉ có hiệu ứng tạm thời, sẽ mất khi khởi động lại.

Có thể cần cấu hình mạng cho docker để có thể pull image.

Sau đó chạy để tạo cluster
```bash
chmod +x ./setup.sh
./setup.sh
```

### 3.3. Kiểm tra hệ thống
1. Kiểm tra trạng thái các Pod: `kubectl get pods`
2. Kiểm tra kết nối tới Kafka từ xa `ncat -zv bigdata-server 9092`
3. Kiểm tra API Elasticsearch `curl http://bigdata-server:9200`
