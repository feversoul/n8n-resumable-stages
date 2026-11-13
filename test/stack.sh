#!/bin/sh
# test/stack.sh up|down|logs|kill|start   the test stack by hand
# kill and start act on the n8n container only; the tests use them to
# interrupt a run for real.
cd "$(dirname "$0")/.."
action=$1
files="-f docker-compose.yml -f test/docker-compose.yml"
case "$action" in
  up) docker compose --env-file test/test.env $files -p stages-test up -d ;;
  down) docker compose --env-file test/test.env $files -p stages-test down -v ;;
  logs) docker compose --env-file test/test.env $files -p stages-test logs --tail "${TAIL:-40}" n8n ;;
  kill) docker compose --env-file test/test.env $files -p stages-test kill n8n ;;
  start) docker compose --env-file test/test.env $files -p stages-test start n8n ;;
esac
