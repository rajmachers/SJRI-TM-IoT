#!/usr/bin/env bash
set -euo pipefail
cd /home/iotuser/sjri-live
RUN_LOCAL_DOCKER=true TB_CONTAINER=christ OUT_FILE=/home/iotuser/sjri-live/sjri_live_data.json ./sjri_tb_live_json_pull.sh >> /home/iotuser/sjri-live/refresh.log 2>&1
