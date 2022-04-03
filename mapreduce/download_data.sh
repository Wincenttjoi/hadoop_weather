CURRENT_DIR=$(dirname "$0")
DATA_DIR="$CURRENT_DIR/resources/raw_data"

echo "DATA_DIR = $DATA_DIR"

python3 "${CURRENT_DIR}/script.py" "${DATA_DIR}"