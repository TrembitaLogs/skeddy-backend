#!/bin/bash
# init-letsencrypt.sh — Initial SSL certificate generation for Skeddy
#
# Solves the nginx + certbot bootstrap problem:
#   nginx needs SSL certs to start, but certbot needs nginx for ACME challenge.
#
# Solution:
#   1. Verify DNS records point to this server
#   2. Create temporary self-signed certificate
#   3. Start nginx with temporary certificate
#   4. Request real certificate from Let's Encrypt
#   5. Reload nginx with the real certificate
#
# After initial setup, renewal is automatic:
#   - certbot container checks for renewal every 12 hours
#   - nginx reloads every 6 hours to pick up renewed certificates
#
# Usage:
#   ./scripts/init-letsencrypt.sh                          # Production
#   ./scripts/init-letsencrypt.sh --staging                # Staging (test)
#   ./scripts/init-letsencrypt.sh --email admin@skeddy.net # Override email
#
# Environment:
#   CERTBOT_EMAIL  — Email for Let's Encrypt (alternative to --email)

set -euo pipefail

# --- Configuration ---
PRIMARY_DOMAIN="beta.skeddy.net"
DOMAINS=("beta.skeddy.net" "api.beta.skeddy.net" "admin.beta.skeddy.net")
EMAIL="${CERTBOT_EMAIL:-}"
STAGING=0
DATA_PATH="./certbot"

# --- Functions ---

log() {
    echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] INIT-CERT: $1"
}

usage() {
    cat <<'USAGE'
Usage: scripts/init-letsencrypt.sh [OPTIONS]

Initial SSL certificate generation for Skeddy backend.

Options:
  --staging          Use Let's Encrypt staging environment (for testing)
  --email EMAIL      Email for Let's Encrypt notifications
                     (overrides CERTBOT_EMAIL env var)
  --help, -h         Show this help message

Environment:
  CERTBOT_EMAIL      Email for Let's Encrypt (alternative to --email)

Examples:
  # First deployment (production):
  ./scripts/init-letsencrypt.sh --email admin@skeddy.net

  # Test with staging first:
  ./scripts/init-letsencrypt.sh --staging --email admin@skeddy.net

  # Using environment variable:
  export CERTBOT_EMAIL=admin@skeddy.net
  ./scripts/init-letsencrypt.sh

Prerequisites:
  - Docker and Docker Compose installed
  - DNS A records for all domains pointing to this server:
    * beta.skeddy.net
    * api.beta.skeddy.net
    * admin.beta.skeddy.net
  - Ports 80 and 443 available

Domains:
  The certificate covers: beta.skeddy.net, api.beta.skeddy.net,
  admin.beta.skeddy.net (SAN certificate).

After initial setup:
  - Certificate renewal: certbot container checks every 12h
  - nginx reload: every 6h (picks up renewed certificates)
  - Certificates stored in: ./certbot/conf/

Troubleshooting:
  DNS not resolving      Verify A records: dig beta.skeddy.net
  Port 80 in use         Check: ss -tlnp | grep :80
  Certbot rate limits    Use --staging for testing first
  Renewal issues         Check: docker compose logs certbot
  Certificate status     openssl s_client -connect beta.skeddy.net:443
  Re-issue certificate   Remove ./certbot/conf/live/beta.skeddy.net/ and re-run
USAGE
}

cleanup_on_error() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log "ERROR: Script failed with exit code $exit_code"
        log "Check container states: docker compose ps"
        log "Check logs: docker compose logs nginx certbot"
    fi
}

check_prerequisites() {
    if ! command -v docker &>/dev/null; then
        log "ERROR: Docker is not installed"
        exit 1
    fi

    if ! docker compose version &>/dev/null; then
        log "ERROR: Docker Compose is not available"
        exit 1
    fi

    if [ ! -f "docker-compose.yml" ]; then
        log "ERROR: docker-compose.yml not found"
        log "Run this script from the project root directory"
        exit 1
    fi

    if ! docker info &>/dev/null; then
        log "ERROR: Docker daemon is not running or current user lacks permissions"
        exit 1
    fi
}

check_dns() {
    log "Verifying DNS records..."
    local failed=0

    for domain in "${DOMAINS[@]}"; do
        if command -v dig &>/dev/null; then
            local ip
            ip=$(dig +short "$domain" A | head -1)
            if echo "$ip" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$'; then
                log "  $domain -> $ip"
            else
                log "ERROR: DNS A record not found for $domain"
                failed=1
            fi
        elif command -v host &>/dev/null; then
            if host "$domain" &>/dev/null; then
                log "  $domain -> OK"
            else
                log "ERROR: DNS resolution failed for $domain"
                failed=1
            fi
        else
            log "WARNING: Neither 'dig' nor 'host' available, skipping DNS check"
            return 0
        fi
    done

    if [ "$failed" -eq 1 ]; then
        log "ERROR: DNS verification failed"
        log "Ensure A records point to this server's IP address"
        log "DNS propagation may take up to 48 hours after changes"
        exit 1
    fi

    log "DNS verification passed"
}

# Returns 0 if a valid production Let's Encrypt certificate exists
check_existing_certificate() {
    local cert_path="$DATA_PATH/conf/live/$PRIMARY_DOMAIN/fullchain.pem"

    if [ ! -f "$cert_path" ]; then
        return 1
    fi

    local issuer
    issuer=$(openssl x509 -in "$cert_path" -noout -issuer 2>/dev/null || echo "")

    # Staging certificates should be replaceable
    if echo "$issuer" | grep -qi "STAGING"; then
        log "Staging certificate found — will be replaced"
        return 1
    fi

    # Valid production Let's Encrypt certificate
    if echo "$issuer" | grep -qi "Let's Encrypt"; then
        return 0
    fi

    # Self-signed or unknown issuer — allow replacement
    return 1
}

remove_existing_certificate() {
    log "Removing existing certificate files..."
    rm -rf "${DATA_PATH:?}/conf/live/$PRIMARY_DOMAIN"
    rm -rf "${DATA_PATH:?}/conf/archive/$PRIMARY_DOMAIN"
    rm -f "${DATA_PATH:?}/conf/renewal/$PRIMARY_DOMAIN.conf"
}

create_dummy_certificate() {
    log "Creating temporary self-signed certificate..."
    local cert_dir="$DATA_PATH/conf/live/$PRIMARY_DOMAIN"

    mkdir -p "$cert_dir"

    openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
        -keyout "$cert_dir/privkey.pem" \
        -out "$cert_dir/fullchain.pem" \
        -subj "/CN=localhost" 2>/dev/null

    log "Temporary certificate created at $cert_dir/"
}

request_certificate() {
    log "Requesting certificate from Let's Encrypt..."

    local domain_args=()
    for domain in "${DOMAINS[@]}"; do
        domain_args+=(-d "$domain")
    done

    local staging_args=()
    if [ "$STAGING" -eq 1 ]; then
        staging_args=(--staging)
        log "Using Let's Encrypt STAGING environment"
        log "Staging certificates are NOT trusted by browsers"
    fi

    docker compose run --rm --entrypoint "" certbot \
        certbot certonly --webroot \
            -w /var/www/certbot \
            --email "$EMAIL" \
            "${domain_args[@]}" \
            --agree-tos \
            --no-eff-email \
            --non-interactive \
            "${staging_args[@]}"
}

# --- Main ---

main() {
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --staging)
                STAGING=1
                shift
                ;;
            --email)
                if [ -z "${2:-}" ]; then
                    log "ERROR: --email requires a value"
                    exit 1
                fi
                EMAIL="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log "ERROR: Unknown option: $1"
                echo ""
                usage
                exit 1
                ;;
        esac
    done

    log "Starting SSL certificate initialization..."
    log "Primary domain: $PRIMARY_DOMAIN"
    log "All domains: ${DOMAINS[*]}"

    # Validate email
    if [ -z "$EMAIL" ]; then
        log "ERROR: Email is required for Let's Encrypt notifications"
        log "Set CERTBOT_EMAIL env var or use --email flag"
        exit 1
    fi
    log "Email: $EMAIL"

    # Check prerequisites
    check_prerequisites

    # Check if valid production cert already exists
    if check_existing_certificate; then
        log "Valid Let's Encrypt production certificate already exists for $PRIMARY_DOMAIN"
        log "Renewal is handled automatically by the certbot container"
        log "To force re-issue, remove $DATA_PATH/conf/live/$PRIMARY_DOMAIN/ and re-run"
        exit 0
    fi

    # Verify DNS records
    check_dns

    # Set up error handler
    trap cleanup_on_error EXIT

    # Clean up any existing non-production certificate (staging, self-signed, etc.)
    if [ -d "$DATA_PATH/conf/live/$PRIMARY_DOMAIN" ]; then
        remove_existing_certificate
    fi

    # Ensure certbot directories exist for volume mounts
    mkdir -p "$DATA_PATH/conf"
    mkdir -p "$DATA_PATH/www"

    # Step 1: Create temporary self-signed certificate so nginx can start
    create_dummy_certificate

    # Step 2: Start nginx (and its dependencies: app, db, redis)
    log "Starting nginx with temporary certificate..."
    docker compose up -d nginx

    # Step 3: Wait for nginx to be ready
    log "Waiting for nginx to start..."
    local retries=15
    local nginx_ready=0
    while [ $retries -gt 0 ]; do
        if docker compose exec nginx nginx -t &>/dev/null; then
            nginx_ready=1
            break
        fi
        retries=$((retries - 1))
        sleep 2
    done

    if [ "$nginx_ready" -eq 0 ]; then
        log "ERROR: nginx failed to start within 30 seconds"
        log "Check logs: docker compose logs nginx"
        exit 1
    fi
    log "nginx is running"

    # Step 4: Remove temporary certificate
    # nginx keeps the old cert in memory — the ACME challenge uses HTTP (port 80)
    # so SSL cert presence on disk is not needed for the verification step
    remove_existing_certificate

    # Step 5: Request real certificate from Let's Encrypt
    request_certificate

    # Step 6: Reload nginx to pick up the real certificate
    log "Reloading nginx with real certificate..."
    docker compose exec nginx nginx -s reload

    # Clear error handler
    trap - EXIT

    # Done
    log "============================================"
    log "SSL certificate successfully obtained!"
    log "============================================"
    log ""
    log "Certificate: $DATA_PATH/conf/live/$PRIMARY_DOMAIN/"
    log "Domains:     ${DOMAINS[*]}"
    if [ "$STAGING" -eq 1 ]; then
        log "Type:        STAGING (not trusted by browsers)"
        log ""
        log "To get a production certificate, re-run without --staging:"
        log "  ./scripts/init-letsencrypt.sh --email $EMAIL"
    else
        log "Type:        PRODUCTION"
    fi
    log ""
    log "Automatic renewal is already configured:"
    log "  - certbot checks for renewal every 12 hours"
    log "  - nginx reloads every 6 hours to apply new certificates"
    log ""
    log "Verify: openssl s_client -connect $PRIMARY_DOMAIN:443 -servername $PRIMARY_DOMAIN </dev/null 2>/dev/null | openssl x509 -noout -dates -issuer"
}

main "$@"
