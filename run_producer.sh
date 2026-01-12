#!/bin/bash
set -e   # Dừng script ngay khi có lỗi

# ==================== CẤU HÌNH ====================
NAMESPACE="${NAMESPACE:-default}"              # Namespace Kubernetes (mặc định: default)
DATA_FILE="${DATA_FILE:-data-sources/covid_0.csv}"  # File dữ liệu CSV dùng để gửi lên Kafka

# ==================== CHỌN CHẾ ĐỘ CHẠY ====================
# Tham số truyền vào khi chạy script:
#   batch     : gửi toàn bộ dữ liệu nhanh (mặc định)
#   streaming : mô phỏng gửi dữ liệu theo từng ngày
MODE="${1:-batch}"

# ==================== MÀU SẮC HIỂN THỊ LOG ====================
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

# ==================== KIỂM TRA CLUSTER KUBERNETES ====================
# Kiểm tra xem cluster k3d / Kubernetes có đang chạy không
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

# ==================== CẤU HÌNH THEO CHẾ ĐỘ ====================
if [ "$MODE" = "streaming" ]; then
    # Chế độ streaming: gửi dữ liệu theo từng ngày
    JOB_NAME="covid-producer-streaming"
    SOURCE_FILE="src/kafka_producer_streaming.py"
    CONFIGMAP_NAME="producer-streaming-script"
    MANIFEST_FILE="k8s-manifests/producer-streaming.yaml"
    POD_LABEL="app=covid-producer-streaming"
    SCRIPT_MOUNT_NAME="kafka_producer_streaming.py"
    log_info "Chế độ: STREAMING (mô phỏng theo ngày)"
else
    # Chế độ batch: gửi toàn bộ dữ liệu nhanh
    JOB_NAME="covid-producer-job"
    SOURCE_FILE="src/kafka_producer.py"
    CONFIGMAP_NAME="producer-script"
    MANIFEST_FILE="k8s-manifests/producer.yaml"
    POD_LABEL="job-name=covid-producer-job"
    SCRIPT_MOUNT_NAME="kafka_producer.py"
    log_info "Chế độ: BATCH (gửi toàn bộ dữ liệu nhanh)"
fi

# ==================== KIỂM TRA FILE CẦN THIẾT ====================
# Kiểm tra file source producer
if [ ! -f "$SOURCE_FILE" ]; then
    log_error "Không tìm thấy file source: $SOURCE_FILE"
    exit 1
fi

# Kiểm tra file dữ liệu CSV
if [ ! -f "$DATA_FILE" ]; then
    log_error "Không tìm thấy file dữ liệu: $DATA_FILE"
    exit 1
fi

# Kiểm tra file manifest Kubernetes
if [ ! -f "$MANIFEST_FILE" ]; then
    log_error "Không tìm thấy file manifest: $MANIFEST_FILE"
    exit 1
fi

log_info "================================================"
log_info "Triển khai Kafka Producer - Chế độ $MODE"
log_info "================================================"
log_info "Job: $JOB_NAME"
log_info "File source: $SOURCE_FILE"
log_info "File dữ liệu: $DATA_FILE"
log_info "================================================"

# Kiểm tra cluster trước khi deploy
check_cluster

# ==================== BƯỚC 1: XOÁ JOB CŨ (NẾU CÓ) ====================
if kubectl get job $JOB_NAME -n $NAMESPACE &>/dev/null; then
    log_info "Đang xoá Job cũ..."
    kubectl delete job $JOB_NAME -n $NAMESPACE
    log_info "Đang chờ Pod cũ kết thúc..."
    kubectl wait --for=delete pod -l $POD_LABEL -n $NAMESPACE --timeout=60s 2>/dev/null || true
    sleep 2
fi

# ==================== BƯỚC 2: CẬP NHẬT CONFIGMAP ====================
log_info "Đang cập nhật ConfigMap..."
kubectl delete configmap $CONFIGMAP_NAME -n $NAMESPACE --ignore-not-found
kubectl create configmap $CONFIGMAP_NAME \
    --from-file=$SCRIPT_MOUNT_NAME=$SOURCE_FILE \
    -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Tạo ConfigMap thất bại"
    exit 1
fi

# ==================== BƯỚC 3: TẠO JOB ====================
log_info "Đang tạo Job từ file manifest..."
kubectl create -f $MANIFEST_FILE -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Tạo Job thất bại"
    exit 1
fi

# ==================== BƯỚC 4: CHỜ POD ĐƯỢC TẠO ====================
log_info "Đang chờ Pod được tạo..."
RETRY_COUNT=0
MAX_RETRIES=30

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    POD_NAME=$(kubectl get pods -l $POD_LABEL -n $NAMESPACE -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)
    
    if [ -n "$POD_NAME" ]; then
        log_info "✓ Pod đã được tạo: $POD_NAME"
        break
    fi
    
    RETRY_COUNT=$((RETRY_COUNT + 1))
    sleep 2
done

if [ -z "$POD_NAME" ]; then
    log_error "Không tìm thấy Pod sau ${MAX_RETRIES} lần thử"
    log_error "Kiểm tra trạng thái Job: kubectl describe job $JOB_NAME -n $NAMESPACE"
    exit 1
fi

# ==================== BƯỚC 5: CHỜ POD SẴN SÀNG ====================
log_info "Đang chờ Pod sẵn sàng..."
kubectl wait --for=condition=Ready pod/$POD_NAME -n $NAMESPACE --timeout=120s

if [ $? -ne 0 ]; then
    log_error "Pod không chuyển sang trạng thái Ready"
    log_error "Kiểm tra Pod: kubectl describe pod $POD_NAME -n $NAMESPACE"
    exit 1
fi

# ==================== BƯỚC 6: COPY FILE DỮ LIỆU VÀO POD ====================
log_info "Đang copy file dữ liệu vào Pod..."
kubectl cp $DATA_FILE $NAMESPACE/$POD_NAME:/app/data/covid_0.csv

if [ $? -ne 0 ]; then
    log_error "Copy file dữ liệu thất bại"
    exit 1
fi

log_info "✓ Copy file dữ liệu thành công"

# ==================== BƯỚC 7: XEM LOG PRODUCER ====================
log_info "================================================"
log_info "Đang xem log Producer (Ctrl+C để thoát)..."
log_info "================================================"
echo ""

kubectl logs -f $POD_NAME -n $NAMESPACE

# Script tiếp tục sau khi log kết thúc hoặc người dùng Ctrl+C
echo ""
log_info "================================================"
log_info "Job Producer đã hoàn thành hoặc bị ngắt"
log_info "================================================"
log_info "Kiểm tra trạng thái Job:"
echo "  kubectl get job $JOB_NAME -n $NAMESPACE"
echo ""
log_info "Xem lại log:"
echo "  kubectl logs $POD_NAME -n $NAMESPACE"
