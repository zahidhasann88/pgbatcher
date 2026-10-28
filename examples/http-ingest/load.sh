#!/usr/bin/env bash
# Simple load generator; requires curl. Unique event IDs allow repeatable runs.
set -eu
BASE=${BASE_URL:-http://localhost:8080}
N=${N:-1000}
CONCURRENCY=${CONCURRENCY:-32}
printf 'Sending %s events with up to %s concurrent curl processes to %s\n' "$N" "$CONCURRENCY" "$BASE"
i=1
while [ "$i" -le "$N" ]; do
  while [ "$(jobs -rp | wc -l)" -ge "$CONCURRENCY" ]; do wait -n; done
  (curl -fsS -X POST "$BASE/events" -H 'content-type: application/json' \
    --data "{\"id\":\"load-$i\",\"kind\":\"load\",\"payload\":\"event-$i\"}" >/dev/null) &
  i=$((i+1))
done
wait
printf 'Done. Check the database with: SELECT count(*) FROM events WHERE kind=\x27load\x27;\n'
