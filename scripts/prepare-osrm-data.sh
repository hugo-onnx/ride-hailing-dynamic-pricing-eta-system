set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/osrm-data"

OSM_URL="https://download.geofabrik.de/europe/spain/madrid-latest.osm.pbf"
OSM_FILE="madrid-latest.osm.pbf"
OSRM_BASE="madrid-latest"

echo "=== OSRM Data Preparation for Madrid Region ==="
echo "Data directory: $DATA_DIR"
echo ""

mkdir -p "$DATA_DIR"
cd "$DATA_DIR"

if [ -f "$OSM_FILE" ]; then
    FILE_SIZE=$(stat -f%z "$OSM_FILE" 2>/dev/null || stat -c%s "$OSM_FILE" 2>/dev/null || echo "0")
    if [ "$FILE_SIZE" -lt 1000000 ]; then
        echo "Removing corrupted/incomplete download ($FILE_SIZE bytes)..."
        rm -f "$OSM_FILE"
    fi
fi

if [ ! -f "$OSM_FILE" ]; then
    echo "[1/4] Downloading Madrid community OSM data from Geofabrik..."
    echo "URL: $OSM_URL"
    
    curl -L -f -o "$OSM_FILE" "$OSM_URL"
    
    FILE_SIZE=$(stat -f%z "$OSM_FILE" 2>/dev/null || stat -c%s "$OSM_FILE" 2>/dev/null || echo "0")
    if [ "$FILE_SIZE" -lt 1000000 ]; then
        echo "Error: Downloaded file is too small ($FILE_SIZE bytes). Download may have failed."
        rm -f "$OSM_FILE"
        exit 1
    fi
    echo "Downloaded: $(ls -lh "$OSM_FILE" | awk '{print $5}')"
else
    echo "[1/4] Madrid OSM data already exists, skipping download"
    ls -lh "$OSM_FILE"
fi

if command -v podman &> /dev/null; then
    CONTAINER_CMD="podman"
elif command -v docker &> /dev/null; then
    CONTAINER_CMD="docker"
else
    echo "Error: Neither podman nor docker found. Please install one."
    exit 1
fi

echo "Using container runtime: $CONTAINER_CMD"

PLATFORM_FLAG=""
MEMORY_FLAG=""
if [[ "$(uname -m)" == "arm64" ]] && [[ "$(uname -s)" == "Darwin" ]]; then
    echo "Detected ARM Mac - using platform flag and memory limit"
    PLATFORM_FLAG="--platform linux/amd64"
    MEMORY_FLAG="-m 6g"
fi

if [ ! -f "${OSRM_BASE}.osrm" ]; then
    echo "[2/4] Extracting routing graph (this takes ~3-5 minutes)..."
    $CONTAINER_CMD run $PLATFORM_FLAG $MEMORY_FLAG -t -v "$DATA_DIR:/data" osrm/osrm-backend \
        osrm-extract -p /opt/car.lua /data/$OSM_FILE
else
    echo "[2/4] Routing graph already extracted, skipping"
fi

if [ ! -f "${OSRM_BASE}.osrm.partition" ]; then
    echo "[3/4] Partitioning graph for MLD algorithm..."
    $CONTAINER_CMD run $PLATFORM_FLAG $MEMORY_FLAG -t -v "$DATA_DIR:/data" osrm/osrm-backend \
        osrm-partition /data/${OSRM_BASE}.osrm
else
    echo "[3/4] Graph already partitioned, skipping"
fi

if [ ! -f "${OSRM_BASE}.osrm.cell_metrics" ]; then
    echo "[4/4] Customizing graph..."
    $CONTAINER_CMD run $PLATFORM_FLAG $MEMORY_FLAG -t -v "$DATA_DIR:/data" osrm/osrm-backend \
        osrm-customize /data/${OSRM_BASE}.osrm
else
    echo "[4/4] Graph already customized, skipping"
fi

echo ""
echo "=== OSRM data preparation complete! ==="
echo ""
echo "Generated files in $DATA_DIR:"
ls -lh "$DATA_DIR"/${OSRM_BASE}.osrm* 2>/dev/null | head -10
echo ""
echo "You can now start the stack with:"
echo "  podman-compose up -d"
echo ""
echo "Test OSRM with (Madrid city center to airport):"
echo "  curl 'http://localhost:5000/route/v1/driving/-3.7038,40.4168;-3.5673,40.4722'"