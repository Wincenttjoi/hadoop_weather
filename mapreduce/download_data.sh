CURRENT_DIR=$(dirname "$0")
RESOURCE_DIR="$CURRENT_DIR/resources"
DATA_DIR="$RESOURCE_DIR/raw_data"

echo "CURRENT_DIR" = $CURRENT_DIR
echo "RESOURCE_DIR = $RESOURCE_DIR"
echo "DATA_DIR = $DATA_DIR"

python3 "${CURRENT_DIR}/script.py" "${DATA_DIR}"

python3 "${RESOURCE_DIR}/data_health_checking.py" "${DATA_DIR}"