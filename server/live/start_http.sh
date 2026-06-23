#!/usr/bin/env bash
set -euo pipefail
pgrep -f "python3 -m http.server 8088 --bind 0.0.0.0 --directory /home/iotuser/sjri-live" >/dev/null 2>&1 || \
  nohup python3 -m http.server 8088 --bind 0.0.0.0 --directory /home/iotuser/sjri-live > /home/iotuser/sjri-live/http.log 2>&1 &
