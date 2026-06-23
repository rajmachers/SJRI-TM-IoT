#!/bin/bash
set -e

SERVER="root@64.227.171.244"
REMOTE_DIR="/opt/iot-platform/docker"
LOCAL_BRIDGE="server/bridge/christ_bridge.py"

echo "→ Copying christ_bridge.py to server..."
scp "$LOCAL_BRIDGE" "$SERVER:$REMOTE_DIR/christ_bridge.py"

echo "→ Rebuilding and restarting christ_bridge on server..."
ssh "$SERVER" "cd $REMOTE_DIR && ./rerun"

echo "✓ Deployed successfully"
