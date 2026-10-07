#!/usr/bin/env bash
# Pod entrypoint: Xvfb + scraper.py, then announce this pod to the Article
# Innovator scraper registries for as long as the scraper answers.
#
# Registration is the same registry the GitHub runners used (the
# url_scraper_service_url system variable, through article-management's
# /health/ endpoint), so nothing downstream changes: the chain, sitemap and
# selector code round-robin over whatever URLs are registered. It re-registers
# every REGISTER_INTERVAL seconds because the registry prunes any URL whose
# health check fails, and a pod that was briefly unready must come back on its
# own. Registration is a read-modify-write, so five pods starting at once can
# drop each other's URL; the loop heals that within a minute.
set -u

PORT="${SCRAPER_PORT:-8814}"
HEALTH="http://127.0.0.1:${PORT}/url-scraper-service/api/v1/health/"
PUBLIC_URL="${SCRAPER_PUBLIC_URL:-}"
BACKENDS="${REGISTRY_BACKENDS:-}"
REGISTER_INTERVAL="${REGISTER_INTERVAL:-60}"

Xvfb "${DISPLAY}" -screen 0 1920x1080x24 -nolisten tcp >/dev/null 2>&1 &
XVFB_PID=$!

python3 /app/scraper.py \
  --port "${PORT}" \
  --chrome "${CHROME_BIN}" \
  --extension "${CF_AUTOCLICK_DIR}" \
  --max-concurrent "${MAX_CONCURRENT:-3}" \
  --timeout "${SCRAPE_TIMEOUT:-60}" &
SCRAPER_PID=$!

announce() {  # $1 = register | deregister
  [ -n "${PUBLIC_URL}" ] || return 0
  for backend in ${BACKENDS}; do
    curl -s -m 15 -X POST "${backend}/article-management-service/api/v1/health/" \
      -H 'Content-Type: application/json' \
      -d "{\"scraper_url\":\"${PUBLIC_URL}\",\"action\":\"$1\"}" >/dev/null 2>&1 \
      || echo "[registry] $1 at ${backend} failed"
  done
}

shutdown() {
  echo "[entrypoint] stopping: deregistering ${PUBLIC_URL}"
  announce deregister
  kill "${SCRAPER_PID}" "${XVFB_PID}" 2>/dev/null
  wait "${SCRAPER_PID}" 2>/dev/null
  exit 0
}
trap shutdown TERM INT

for _ in $(seq 1 60); do
  curl -sf -m 3 "${HEALTH}" >/dev/null 2>&1 && break
  sleep 1
done
echo "[entrypoint] scraper up on :${PORT}; announcing ${PUBLIC_URL:-<no public url>} to: ${BACKENDS:-<none>}"

while kill -0 "${SCRAPER_PID}" 2>/dev/null; do
  curl -sf -m 3 "${HEALTH}" >/dev/null 2>&1 && announce register
  sleep "${REGISTER_INTERVAL}" &
  wait $!
done
echo "[entrypoint] scraper process exited"
exit 1
