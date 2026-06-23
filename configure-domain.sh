#!/bin/bash
# SJRI IoT Platform — Add Domain + SSL to an already-running server
# Run from your local Mac: ./configure-domain.sh [user@server-ip]
# Requires: setup.sh already completed on the target server.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}→${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
warn()    { echo -e "${YELLOW}⚠${NC} $*"; }
die()     { echo -e "${RED}✗${NC} $*" >&2; exit 1; }

prompt() { local var=$1 msg=$2 default=$3; read -r -p "  $msg [$default]: " val; eval "$var=\"${val:-$default}\""; }

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║       SJRI IoT Platform — Domain + SSL Setup            ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Target server ──────────────────────────────────────────────────────────
TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
  read -r -p "  Target server (user@ip): " TARGET
fi
[[ -z "$TARGET" ]] && die "No target server provided."

info "Checking SSH connection..."
ssh -o ConnectTimeout=10 -o BatchMode=yes "$TARGET" "echo ok" > /dev/null 2>&1 \
  || die "Cannot SSH to $TARGET."
success "SSH OK"
echo ""

# ── 2. Read existing .env for ports ──────────────────────────────────────────
ENV_FILE="$REPO_ROOT/server/docker/.env"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  info "Loaded ports from $ENV_FILE (TB: ${TB_HTTP_PORT:-9090}, Live: ${LIVE_PORT:-8088})"
else
  warn ".env not found — enter ports manually"
  prompt TB_HTTP_PORT "ThingsBoard HTTP port" "9090"
  prompt LIVE_PORT    "Live dashboard port"   "8088"
fi
TB_HTTP_PORT="${TB_HTTP_PORT:-9090}"
LIVE_PORT="${LIVE_PORT:-8088}"
echo ""

# ── 3. Domain details ─────────────────────────────────────────────────────────
echo "--- Domain Configuration ---"
prompt DOMAIN "Domain name (e.g. tracking.client.in)" ""
[[ -z "$DOMAIN" ]] && die "Domain name is required."

echo ""
echo "--- SSL Certificate ---"
echo "  1) Use Let's Encrypt (certbot) — automatic, requires domain pointing to this server"
echo "  2) Manual certificate files   — you provide cert + key paths"
read -r -p "  SSL method [1/2]: " SSL_METHOD
SSL_METHOD="${SSL_METHOD:-1}"
echo ""

if [[ "$SSL_METHOD" == "1" ]]; then
  # ── Let's Encrypt ──────────────────────────────────────────────────────────
  prompt LE_EMAIL "Email for Let's Encrypt alerts" ""
  [[ -z "$LE_EMAIL" ]] && die "Email is required for Let's Encrypt."

  info "Installing certbot on $TARGET..."
  ssh "$TARGET" bash <<'REMOTE'
set -e
if command -v certbot &>/dev/null; then
  echo "  certbot already installed"
  exit 0
fi
apt-get install -y -qq certbot python3-certbot-nginx
echo "  certbot installed"
REMOTE
  success "certbot ready"

  info "Obtaining certificate for $DOMAIN..."
  ssh "$TARGET" "certbot certonly --nginx --non-interactive --agree-tos -m $LE_EMAIL -d $DOMAIN"
  SSL_CERT="/etc/letsencrypt/live/$DOMAIN/fullchain.pem"
  SSL_KEY="/etc/letsencrypt/live/$DOMAIN/privkey.pem"
  success "Certificate obtained"

else
  # ── Manual cert ────────────────────────────────────────────────────────────
  prompt LOCAL_CERT "Local path to fullchain cert file" ""
  prompt LOCAL_KEY  "Local path to private key file"    ""
  [[ -z "$LOCAL_CERT" || -z "$LOCAL_KEY" ]] && die "Both cert and key paths are required."
  [[ -f "$LOCAL_CERT" ]] || die "Cert file not found: $LOCAL_CERT"
  [[ -f "$LOCAL_KEY"  ]] || die "Key file not found: $LOCAL_KEY"

  REMOTE_SSL_DIR="/etc/nginx/ssl/$DOMAIN"
  info "Uploading certificates to $TARGET:$REMOTE_SSL_DIR ..."
  ssh "$TARGET" "mkdir -p $REMOTE_SSL_DIR"
  scp "$LOCAL_CERT" "$TARGET:$REMOTE_SSL_DIR/fullchain.crt"
  scp "$LOCAL_KEY"  "$TARGET:$REMOTE_SSL_DIR/private.key"
  SSL_CERT="$REMOTE_SSL_DIR/fullchain.crt"
  SSL_KEY="$REMOTE_SSL_DIR/private.key"
  success "Certificates uploaded"
fi

# ── 4. Generate and deploy nginx config ───────────────────────────────────────
info "Deploying nginx config for $DOMAIN..."
NGINX_CONF=$(sed \
  -e "s|{{DOMAIN}}|$DOMAIN|g" \
  -e "s|{{TB_HTTP_PORT}}|$TB_HTTP_PORT|g" \
  -e "s|{{LIVE_PORT}}|$LIVE_PORT|g" \
  -e "s|{{SSL_CERT}}|$SSL_CERT|g" \
  -e "s|{{SSL_KEY}}|$SSL_KEY|g" \
  "$REPO_ROOT/nginx/domain-ssl.conf.template")

CONF_NAME="${DOMAIN//./_}"

ssh "$TARGET" bash <<REMOTE
set -e
cat > /etc/nginx/sites-available/${CONF_NAME}.conf <<'CONF'
$NGINX_CONF
CONF
ln -sf /etc/nginx/sites-available/${CONF_NAME}.conf /etc/nginx/sites-enabled/${CONF_NAME}.conf
# Remove IP-only config if present — domain config takes over
rm -f /etc/nginx/sites-enabled/iot-platform.conf
nginx -t
systemctl reload nginx
echo "  nginx reloaded"
REMOTE
success "nginx configured for $DOMAIN"

# ── 5. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           Domain Configuration Complete                  ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
success "ThingsBoard UI  : https://$DOMAIN/"
success "Live Dashboard  : https://$DOMAIN/live/"
echo ""
if [[ "$SSL_METHOD" == "1" ]]; then
  warn "Let's Encrypt auto-renews via certbot cron. Verify with:"
  echo "  ssh $TARGET 'certbot renew --dry-run'"
fi
echo ""
