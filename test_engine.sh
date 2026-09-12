#!/bin/bash

NUM_PAYLOADS=30
WEBHOOK_URL="https://webhook.site/4604bf99-eb2e-403c-8e41-f8e26e9b7000"
LANGUAGE="python"
CODE="import time; time.sleep(10); print('Execution Complete!')"
# ==========================================

echo "🚀 Firing $NUM_PAYLOADS payloads to localhost:30080..."

for i in $(seq 1 $NUM_PAYLOADS); do
  curl -s -X POST http://localhost:30080/submit \
  -H "Content-Type: application/json" \
  -d '{
        "id": "test-run-'$i'",
        "language": "'"$LANGUAGE"'",
        "code": "'"$CODE"'",
        "webhook_url": "'"$WEBHOOK_URL"'"
      }' &
done

wait
echo ""
echo "✅ All $NUM_PAYLOADS payloads successfully queued!"