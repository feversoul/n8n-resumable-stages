#!/bin/sh
# Entrypoint of the n8n container in docker-compose.yml.
# First start only: create the Postgres credential, import and publish the
# workflows (a minute or two). Later starts leave your edits alone.
set -eu

MARK=/home/node/.n8n/.stages-imported

if [ ! -f "$MARK" ]; then
  STAGE=$(mktemp -d)
  node /bootstrap/prepare.mjs "$STAGE" /bootstrap/workflows/text-array /bootstrap/workflows/jsonb /bootstrap/lab
  n8n import:credentials --input="$STAGE/credentials.json"
  n8n import:workflow --separate --input="$STAGE/workflows"
  # One CLI call per workflow, sub-workflows first; this is most of the first start.
  while read -r id; do
    n8n publish:workflow --id="$id"
  done < "$STAGE/publish-order.txt"
  rm -rf "$STAGE"
  touch "$MARK"
  echo "stages: workflows imported and published"
fi

exec n8n start
