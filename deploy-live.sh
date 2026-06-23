#!/bin/bash
# Deploy live dashboard files to server.
# Usage:
#   ./deploy-live.sh                  — deploy all files
#   ./deploy-live.sh presentation_v2.html  — deploy a single file

set -e

SERVER="root@64.227.171.244"
REMOTE_DIR="/home/iotuser/sjri-live"
LOCAL_DIR="server/live"

ALL_FILES=(
  index.html
  presentation_v2.html
  presentation_v3.html
  presentation_v4.html
  v2.html
  sjri_tb_live_json_pull.sh
  sjri_tb_live_json_pull_v2.sh
  sjri_tb_live_json_pull_v4.sh
  refresh_json.sh
  refresh_json_v2.sh
  refresh_json_v4.sh
  start_http.sh
  SJRI_PoC_UnknownZone_AliasTemplate_v3.csv
)

if [ $# -eq 0 ]; then
  FILES=("${ALL_FILES[@]}")
  echo "→ Deploying all live files..."
else
  FILES=("$@")
  echo "→ Deploying: ${FILES[*]}"
fi

for f in "${FILES[@]}"; do
  echo "  scp $LOCAL_DIR/$f → $REMOTE_DIR/$f"
  scp "$LOCAL_DIR/$f" "$SERVER:$REMOTE_DIR/$f"
done

echo "✓ Live at https://assettracking.stjohns.in/live/"
