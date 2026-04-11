#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
COMPOSE_FILES="-f docker-compose.yml -f docker-compose.prod.yml"
FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
HEALTH_URL="http://localhost:8000/health"
HEALTH_RETRIES=30
HEALTH_INTERVAL=2

echo "==> Deploying ${FULL_IMAGE}"

# --- 0. Secure .env file permissions ---
if [ -f .env ]; then
  chmod 600 .env
fi

# --- 1. Login to GHCR ---
echo "==> Logging in to GHCR..."
echo "${CR_PAT}" | docker login ghcr.io -u "${CR_USER}" --password-stdin

# --- 2. Pull new image ---
echo "==> Pulling image..."
docker pull "${FULL_IMAGE}"

# --- 3. Export image ref and version for docker-compose.prod.yml ---
export APP_IMAGE="${FULL_IMAGE}"
export APP_VERSION="${IMAGE_TAG}"

# --- 4. Run migrations (with db dependency) ---
echo "==> Running database migrations..."
docker compose ${COMPOSE_FILES} run --rm app \
  sh -c "alembic upgrade head"

# --- 5. Restart app service ---
echo "==> Restarting app service..."
docker compose ${COMPOSE_FILES} up -d --force-recreate app

# --- 6. Health check ---
echo "==> Waiting for health check..."
for i in $(seq 1 ${HEALTH_RETRIES}); do
  if docker compose ${COMPOSE_FILES} exec app \
    python -c "import urllib.request; urllib.request.urlopen('${HEALTH_URL}')" 2>/dev/null; then
    echo "==> Health check passed (attempt ${i}/${HEALTH_RETRIES})"
    break
  fi
  if [ "${i}" -eq "${HEALTH_RETRIES}" ]; then
    echo "ERROR: Health check failed after ${HEALTH_RETRIES} attempts"
    echo "==> Recent logs:"
    docker compose ${COMPOSE_FILES} logs --tail=50 app
    exit 1
  fi
  echo "    Attempt ${i}/${HEALTH_RETRIES} - waiting ${HEALTH_INTERVAL}s..."
  sleep "${HEALTH_INTERVAL}"
done

# --- 7. Cleanup old images ---
echo "==> Cleaning up old images..."
docker image prune -f 2>/dev/null || true

echo "==> Deploy complete: ${FULL_IMAGE}"
