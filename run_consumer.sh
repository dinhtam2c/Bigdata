#!/bin/bash
# run_consumer.sh - Triển khai Kafka Consumer lên Kubernetes
# Sử dụng file có sẵn: src/kafka_consumer.py

set -e  # Thoát ngay khi gặp lỗi

# ==================== CẤU HÌNH ====================
NAMESPACE="${NAMESPACE:-default}"          # Namespace Kubernetes (mặc định: default)
CONFIGMAP_NAME="consumer-script"           # Tên ConfigMap chứa code consumer
DEPLOYMENT_NAME="consumer"                 # Tên Deployment
SOURCE_FILE="src/kafka_consumer.py"        # File nguồn consumer
MANIFEST_FILE="k8s-manifests/consumer.yaml" # File manifest Kubernetes

# ==================== MÀU SẮC LOG ====================
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # Reset màu

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ==================== KIỂM TRA CLUSTER ====================
# Kiểm tra xem cluster Kubernetes (k3d) có đang chạy không
check_cluster() {
    log_info "Đang kiểm tra Kubernetes cluster..."
    
    if ! kubectl cluster-info &>/dev/null; then
        log_error "Kubernetes cluster chưa chạy!"
        log_error ""
        log_error "Vui lòng khởi động cluster trước:"
        log_error "  bash setup.sh"
        log_error ""
        log_error "Hoặc nếu cluster đã tồn tại nhưng đang dừng:"
        log_error "  k3d cluster start bigdata"
        exit 1
    fi
    
    log_info "✓ Cluster đang hoạt động"
}

# ==================== KIỂM TRA FILE ====================
# Kiểm tra file consumer tồn tại
if [ ! -f "$SOURCE_FILE" ]; then
    log_error "Không tìm thấy file nguồn: $SOURCE_FILE"
    exit 1
fi

# Kiểm tra file manifest tồn tại
if [ ! -f "$MANIFEST_FILE" ]; then
    log_error "Không tìm thấy file manifest: $MANIFEST_FILE"
    exit 1
fi

log_info "================================================"
log_info "Triển khai Kafka Consumer lên Kubernetes"
log_info "================================================"
log_info "Namespace: $NAMESPACE"
log_info "File nguồn: $SOURCE_FILE"
log_info "================================================"

# Kiểm tra cluster trước khi deploy
check_cluster

# ==================== BƯỚC 1: XOÁ CONFIGMAP CŨ ====================
log_info "Đang xoá ConfigMap cũ (nếu tồn tại)..."
kubectl delete configmap $CONFIGMAP_NAME -n $NAMESPACE --ignore-not-found
sleep 1

# ==================== BƯỚC 2: TẠO CONFIGMAP MỚI ====================
log_info "Tạo ConfigMap mới từ $SOURCE_FILE..."
kubectl create configmap $CONFIGMAP_NAME \
    --from-file=kafka_consumer.py=$SOURCE_FILE \
    -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Tạo ConfigMap thất bại"
    exit 1
fi

# ==================== BƯỚC 3: APPLY DEPLOYMENT ====================
log_info "Áp dụng file deployment..."
kubectl apply -f $MANIFEST_FILE -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Apply deployment thất bại"
    exit 1
fi

# ==================== BƯỚC 4: RESTART DEPLOYMENT ====================
# Restart để pod load lại code mới từ ConfigMap
log_info "Restart deployment để load code mới..."
kubectl rollout restart deployment/$DEPLOYMENT_NAME -n $NAMESPACE

# ==================== BƯỚC 5: ĐỢI DEPLOYMENT HOÀN THÀNH ====================
log_info "Đang chờ deployment hoàn tất..."
kubectl rollout status deployment/$DEPLOYMENT_NAME -n $NAMESPACE --timeout=120s

if [ $? -eq 0 ]; then
    log_info "================================================"
    log_info "✓ Kafka Consumer đã được deploy thành công!"
    log_info "================================================"
    echo ""
    log_info "Kiểm tra trạng thái pod:"
    echo "  kubectl get pods -l app=consumer -n $NAMESPACE"
    echo ""
    log_info "Xem log consumer:"
    echo "  kubectl logs -l app=consumer -n $NAMESPACE -f"
    echo ""
else
    log_error "Deployment lỗi hoặc quá thời gian chờ"
    log_error "Kiểm tra pod: kubectl describe pods -l app=consumer"
    exit 1
fi
