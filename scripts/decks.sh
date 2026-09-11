#!/usr/bin/env bash
set -euo pipefail

# Resume the existing local demo containers; setup and migration stay in the POC.
lessons=kafka-demo
migration=kafka-demo-migration-live

usage() {
  cat <<'EOF'
Usage: ./scripts/decks.sh lessons|migration|both|stop|status
  lessons    Start the saved workshop broker and lesson deck; stop migration deck
  migration  Start migration deck; stop lesson deck (Kind POC must be running)
  both       Start both decks (Kind POC must be running)
  stop       Stop both decks; leave Kafka and Kubernetes running
  status     Show the saved backend and infrastructure containers

Uses the current Docker context. Does not build, provision, or run a migration.
EOF
}

require_container() {
  if ! docker container inspect "$1" >/dev/null 2>&1; then
    echo "Missing container: $1. Set up this demo first; see README.md." >&2
    exit 1
  fi
}

running() {
  [[ $(docker container inspect --format '{{.State.Running}}' "$1" 2>/dev/null) == true ]]
}

start() {
  if ! running "$1"; then docker start "$1"; fi
}

stop() {
  if running "$1"; then docker stop "$1"; fi
}

wait_for_http() {
  local url=$1
  for ((attempt=0; attempt<30; attempt++)); do
    if curl --fail --silent --max-time 2 "$url" >/dev/null; then return; fi
    sleep 2
  done
  echo "Backend did not become ready: $url. Check docker logs. The other deck was left running." >&2
  exit 1
}

action=${1:-status}
if [[ $# -gt 1 ]]; then usage >&2; exit 2; fi
case "$action" in
  -h|--help|help) usage; exit 0 ;;
  lessons|migration|both|stop|status) ;;
  *) usage >&2; exit 2 ;;
esac

command -v docker >/dev/null || { echo 'Docker is required.' >&2; exit 1; }
echo "Docker context: $(docker context show)"
docker info >/dev/null

if [[ $action == status ]]; then
  docker ps -a --filter "name=^/${lessons}$" --filter "name=^/${migration}$" \
    --filter 'name=^/kafka1$' --filter 'name=^/kafka-proxy-poc-control-plane$' \
    --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
  exit 0
fi

if [[ $action == stop ]]; then
  stop "$lessons"
  stop "$migration"
  exit 0
fi

command -v curl >/dev/null || { echo 'curl is required.' >&2; exit 1; }
# Check all prerequisites before changing either backend.
if [[ $action == lessons || $action == both ]]; then
  require_container "$lessons"
  require_container kafka1
fi
if [[ $action == migration || $action == both ]]; then
  require_container "$migration"
  if ! running kafka-proxy-poc-control-plane; then
    echo 'Start the kind-kafka-proxy-poc POC first; see README.md. No decks changed.' >&2
    exit 1
  fi
fi

if [[ $action == lessons || $action == both ]]; then
  start kafka1
  start "$lessons"
  wait_for_http http://localhost:8080/api/topics
fi
if [[ $action == migration || $action == both ]]; then
  start "$migration"
  wait_for_http http://localhost:18080/api/migration
fi

# Only stop the previous deck after the requested backend responds.
case "$action" in
  lessons) stop "$migration" ;;
  migration) stop "$lessons" ;;
esac

if [[ $action == lessons || $action == both ]]; then
  echo 'Kafka lessons: http://localhost:8080/'
fi
if [[ $action == migration || $action == both ]]; then
  echo 'Migration: http://localhost:18080/migration.html#/migration-flow'
fi
echo 'Backend HTTP is ready. Check live source/connection indicators before presenting.'
