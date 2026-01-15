#!/bin/bash
set -e

NAMESPACE="${NAMESPACE:-default}"
DATA_FILE="${DATA_FILE:-data-sources/covid_0.csv}"

MODE="${1:-batch}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

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

if [ "$MODE" = "streaming" ]; then
    JOB_NAME="covid-producer-streaming"
    SOURCE_FILE="src/kafka_producer_streaming.py"
    CONFIGMAP_NAME="producer-streaming-script"
    MANIFEST_FILE="k8s-manifests/producer-streaming.yaml"
    POD_LABEL="app=covid-producer-streaming"
    SCRIPT_MOUNT_NAME="kafka_producer_streaming.py"
    log_info "Chế độ: STREAMING (mô phỏng theo ngày)"
else
    JOB_NAME="covid-producer-job"
    SOURCE_FILE="src/kafka_producer.py"
    CONFIGMAP_NAME="producer-script"
    MANIFEST_FILE="k8s-manifests/producer.yaml"
    POD_LABEL="job-name=covid-producer-job"
    SCRIPT_MOUNT_NAME="kafka_producer.py"
    log_info "Chế độ: BATCH (gửi toàn bộ dữ liệu nhanh)"
fi

if [ ! -f "$SOURCE_FILE" ]; then
    log_error "Không tìm thấy file source: $SOURCE_FILE"
    exit 1
fi

if [ ! -f "$DATA_FILE" ]; then
    log_error "Không tìm thấy file dữ liệu: $DATA_FILE"
    exit 1
fi
if [ ! -f "$MANIFEST_FILE" ]; then
    log_error "Không tìm thấy file manifest: $MANIFEST_FILE"
    exit 1
fi

log_info "Triển khai Kafka Producer - Chế độ $MODE"
log_info "Job: $JOB_NAME"
log_info "File source: $SOURCE_FILE"
log_info "File dữ liệu: $DATA_FILE"

check_cluster

if kubectl get job $JOB_NAME -n $NAMESPACE &>/dev/null; then
    log_info "Đang xoá Job cũ..."
    kubectl delete job $JOB_NAME -n $NAMESPACE
    log_info "Đang chờ Pod cũ kết thúc..."
    kubectl wait --for=delete pod -l $POD_LABEL -n $NAMESPACE --timeout=60s 2>/dev/null || true
    sleep 2
fi

log_info "Đang cập nhật ConfigMap..."
kubectl delete configmap $CONFIGMAP_NAME -n $NAMESPACE --ignore-not-found
kubectl create configmap $CONFIGMAP_NAME \
    --from-file=$SCRIPT_MOUNT_NAME=$SOURCE_FILE \
    -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Tạo ConfigMap thất bại"
    exit 1
fi

log_info "Đang tạo Job từ file manifest..."
kubectl create -f $MANIFEST_FILE -n $NAMESPACE

if [ $? -ne 0 ]; then
    log_error "Tạo Job thất bại"
    exit 1
fi

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

log_info "Đang chờ Pod sẵn sàng..."
kubectl wait --for=condition=Ready pod/$POD_NAME -n $NAMESPACE --timeout=120s

if [ $? -ne 0 ]; then
    log_error "Pod không chuyển sang trạng thái Ready"
    log_error "Kiểm tra Pod: kubectl describe pod $POD_NAME -n $NAMESPACE"
    exit 1
fi


log_info "Đang copy file dữ liệu vào Pod..."
kubectl cp $DATA_FILE $NAMESPACE/$POD_NAME:/app/data/covid_0.csv

if [ $? -ne 0 ]; then
    log_error "Copy file dữ liệu thất bại"
    exit 1
fi

log_info "✓ Copy file dữ liệu thành công"


log_info "Đang xem log Producer (Ctrl+C để thoát)..."
echo ""

kubectl logs -f $POD_NAME -n $NAMESPACE


echo ""
log_info "Job Producer đã hoàn thành hoặc bị ngắt"
log_info "Kiểm tra trạng thái Job:"
echo "  kubectl get job $JOB_NAME -n $NAMESPACE"
echo ""
log_info "Xem lại log:"
echo "  kubectl logs $POD_NAME -n $NAMESPACE"
