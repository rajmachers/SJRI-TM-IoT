#!/bin/bash
# SJRI IoT Platform — Fresh Server Setup
# Run from your local Mac: ./setup.sh [user@server-ip]
# Provisions a brand-new Ubuntu/Debian server with the full IoT stack.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
REMOTE_DOCKER_DIR="/opt/iot-platform/docker"
REMOTE_LIVE_DIR="/home/iotuser/sjri-live"
REMOTE_DATA_DIR="/opt/iot-platform"

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}→${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
warn()    { echo -e "${YELLOW}⚠${NC} $*"; }
die()     { echo -e "${RED}✗${NC} $*" >&2; exit 1; }

prompt()      { local var=$1 msg=$2 default=$3; read -r -p "  $msg [$default]: " val; eval "$var=\"${val:-$default}\""; }
prompt_secret() { local var=$1 msg=$2; read -r -s -p "  $msg: " val; echo; eval "$var=\"$val\""; }

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║        SJRI IoT Platform — Server Setup                  ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Target server ─────────────────────────────────────────────────────────
TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
  read -r -p "  Target server (user@ip): " TARGET
fi
[[ -z "$TARGET" ]] && die "No target server provided."

info "Checking SSH connection to $TARGET..."
ssh -o ConnectTimeout=10 -o BatchMode=yes "$TARGET" "echo ok" > /dev/null 2>&1 \
  || die "Cannot SSH to $TARGET. Ensure your key is added: ssh-copy-id $TARGET"
success "SSH connection OK"
echo ""

# ── 2. Port configuration ─────────────────────────────────────────────────────
echo "--- Port Configuration ---"
prompt TB_HTTP_PORT  "ThingsBoard HTTP port"  "9090"
prompt MQTT_PORT     "MQTT port"              "1883"
prompt LIVE_PORT     "Live dashboard port"    "8088"
echo ""

# ── 3. Bridge / ThingsBoard credentials ──────────────────────────────────────
echo "--- ThingsBoard Configuration ---"
prompt        TB_USERNAME    "ThingsBoard username"   "sjriiot@sjri.in"
prompt_secret TB_PASSWORD    "ThingsBoard password"
echo ""

echo "--- Kinesis Bridge Configuration ---"
prompt        KINESIS_BASE   "Kinesis base URL"       "alpha.atollkinesis.com/"
prompt        SITE_ID        "Site ID"                "atoll_stjohn"
prompt        CLIENT_ID      "Client ID"              ""
prompt_secret CLIENT_SECRET  "Client Secret"
prompt        DEVICE_TYPE    "Device Type"            "PoC-Tag"
echo ""

# ── 4. Confirm ────────────────────────────────────────────────────────────────
echo "--- Summary ---"
echo "  Server      : $TARGET"
echo "  TB port     : $TB_HTTP_PORT   MQTT: $MQTT_PORT   Live: $LIVE_PORT"
echo "  TB user     : $TB_USERNAME"
echo "  Site ID     : $SITE_ID   Device: $DEVICE_TYPE"
echo "  Kinesis     : $KINESIS_BASE"
echo ""
read -r -p "Proceed with setup? [y/N]: " confirm
[[ "${confirm,,}" == "y" ]] || { echo "Aborted."; exit 0; }
echo ""

# ── 5. Write .env locally (gitignored) ───────────────────────────────────────
ENV_FILE="$REPO_ROOT/server/docker/.env"
cat > "$ENV_FILE" <<EOF
TB_HTTP_PORT=$TB_HTTP_PORT
MQTT_PORT=$MQTT_PORT
TB_USERNAME=$TB_USERNAME
TB_PASSWORD=$TB_PASSWORD
KINESIS_BASE=$KINESIS_BASE
SITE_ID=$SITE_ID
CLIENT_ID=$CLIENT_ID
CLIENT_SECRET=$CLIENT_SECRET
DEVICE_TYPE=$DEVICE_TYPE
EOF
success "Written $ENV_FILE (gitignored)"

# ── 6. Remote: install Docker ─────────────────────────────────────────────────
info "Installing Docker on $TARGET (if not present)..."
ssh "$TARGET" bash <<'REMOTE'
set -e
if command -v docker &>/dev/null; then
  echo "  Docker already installed: $(docker --version)"
  exit 0
fi
echo "  Installing Docker..."
apt-get update -qq
apt-get install -y -qq ca-certificates curl gnupg lsb-release
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update -qq
apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable --now docker
echo "  Docker installed: $(docker --version)"
REMOTE
success "Docker ready"

# ── 7. Remote: create iotuser ─────────────────────────────────────────────────
info "Creating iotuser on $TARGET..."
ssh "$TARGET" bash <<'REMOTE'
set -e
if id iotuser &>/dev/null; then
  echo "  iotuser already exists"
else
  useradd -m -s /bin/bash -G sudo,docker iotuser
  echo "  iotuser created"
fi
# Ensure docker group membership even if user existed
usermod -aG docker iotuser 2>/dev/null || true
REMOTE
success "iotuser ready"

# ── 8. Remote: create directories ────────────────────────────────────────────
info "Creating directories on $TARGET..."
ssh "$TARGET" bash <<REMOTE
set -e
mkdir -p /opt/iot-platform/.mytb-data /opt/iot-platform/.mytb-logs
mkdir -p $REMOTE_DOCKER_DIR
mkdir -p $REMOTE_LIVE_DIR
chown -R iotuser:iotuser /opt/iot-platform $REMOTE_LIVE_DIR
echo "  Directories ready"
REMOTE
success "Directories ready"

# ── 9. Deploy docker files ────────────────────────────────────────────────────
info "Deploying docker files..."
scp "$REPO_ROOT/server/docker/docker-compose.yml" "$TARGET:$REMOTE_DOCKER_DIR/"
scp "$REPO_ROOT/server/docker/Dockerfile"         "$TARGET:$REMOTE_DOCKER_DIR/"
scp "$REPO_ROOT/server/docker/.env"               "$TARGET:$REMOTE_DOCKER_DIR/"
scp "$REPO_ROOT/server/bridge/christ_bridge.py"   "$TARGET:$REMOTE_DOCKER_DIR/"
scp "$REPO_ROOT/server/docker/scripts/"{up,down,build,rerun} "$TARGET:$REMOTE_DOCKER_DIR/"
ssh "$TARGET" "chmod +x $REMOTE_DOCKER_DIR/up $REMOTE_DOCKER_DIR/down $REMOTE_DOCKER_DIR/build $REMOTE_DOCKER_DIR/rerun"
success "Docker files deployed"

# ── 10. Deploy live dashboard files ──────────────────────────────────────────
info "Deploying live dashboard files..."
scp "$REPO_ROOT/server/live/"*.html                    "$TARGET:$REMOTE_LIVE_DIR/" 2>/dev/null || true
scp "$REPO_ROOT/server/live/"*.sh                      "$TARGET:$REMOTE_LIVE_DIR/" 2>/dev/null || true
scp "$REPO_ROOT/server/live/"*.csv                     "$TARGET:$REMOTE_LIVE_DIR/" 2>/dev/null || true
ssh "$TARGET" "chmod +x $REMOTE_LIVE_DIR/*.sh 2>/dev/null || true && chown -R iotuser:iotuser $REMOTE_LIVE_DIR"
success "Live files deployed"

# ── 11. Install nginx ─────────────────────────────────────────────────────────
info "Installing nginx on $TARGET..."
ssh "$TARGET" bash <<'REMOTE'
set -e
if command -v nginx &>/dev/null; then
  echo "  nginx already installed"
  exit 0
fi
apt-get install -y -qq nginx
systemctl enable --now nginx
echo "  nginx installed"
REMOTE
success "nginx ready"

# ── 12. Configure nginx (IP-only) ─────────────────────────────────────────────
info "Configuring nginx for IP-only access..."
NGINX_CONF=$(sed \
  -e "s|{{TB_HTTP_PORT}}|$TB_HTTP_PORT|g" \
  -e "s|{{LIVE_PORT}}|$LIVE_PORT|g" \
  "$REPO_ROOT/nginx/ip-only.conf.template")

ssh "$TARGET" bash <<REMOTE
set -e
cat > /etc/nginx/sites-available/iot-platform.conf <<'CONF'
$NGINX_CONF
CONF
ln -sf /etc/nginx/sites-available/iot-platform.conf /etc/nginx/sites-enabled/iot-platform.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t
systemctl reload nginx
echo "  nginx configured"
REMOTE
success "nginx configured"

# ── 13. Set up live dashboard systemd service ──────────────────────────────────
info "Setting up live dashboard service..."
ssh "$TARGET" bash <<REMOTE
set -e
cat > /etc/systemd/system/sjri-live.service <<'SVC'
[Unit]
Description=SJRI Live Dashboard HTTP Server
After=network.target

[Service]
User=iotuser
ExecStart=/usr/bin/python3 -m http.server $LIVE_PORT --bind 0.0.0.0 --directory $REMOTE_LIVE_DIR
Restart=always
RestartSec=5
StandardOutput=append:$REMOTE_LIVE_DIR/http.log
StandardError=append:$REMOTE_LIVE_DIR/http.log

[Install]
WantedBy=multi-user.target
SVC
systemctl daemon-reload
systemctl enable --now sjri-live
echo "  sjri-live service started"
REMOTE
success "Live dashboard service running"

# ── 14. Start Docker stack ────────────────────────────────────────────────────
info "Pulling ThingsBoard image and starting Docker stack..."
info "(This may take a few minutes on first run — image is ~2.4 GB)"
ssh "$TARGET" "cd $REMOTE_DOCKER_DIR && docker compose pull christ && docker compose up -d"
success "Docker stack started"

# ── 15. Summary ───────────────────────────────────────────────────────────────
SERVER_IP=$(echo "$TARGET" | sed 's/.*@//')
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              Setup Complete                              ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
success "ThingsBoard UI  : http://$SERVER_IP/"
success "Live Dashboard  : http://$SERVER_IP/live/"
success "MQTT            : $SERVER_IP:$MQTT_PORT"
echo ""
warn "ThingsBoard takes ~90 seconds to boot on first start."
warn "Default login: sysadmin@thingsboard.org / sysadmin"
echo ""
warn "To add a domain + SSL later, run:"
echo "  ./configure-domain.sh $TARGET"
echo ""
warn "To set up live data refresh (optional cron), SSH in and run:"
echo "  crontab -u iotuser -e"
echo "  # Add: */5 * * * * $REMOTE_LIVE_DIR/refresh_json_v4.sh >> $REMOTE_LIVE_DIR/refresh.log 2>&1"
echo ""
