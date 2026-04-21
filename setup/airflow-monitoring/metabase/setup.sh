#!/bin/sh
set -e

MB_URL="http://metabase:3000"
ADMIN_EMAIL="admin@transport.local"
ADMIN_PASSWORD="admin123"
ADMIN_FIRST="Admin"
ADMIN_LAST="User"

echo "[metabase-init] Waiting for Metabase API..."

# Get setup token (only available before setup is complete)
TOKEN=$(curl -sf "$MB_URL/api/session/properties" | grep -o '"setup-token":"[^"]*"' | cut -d'"' -f4)

if [ -z "$TOKEN" ]; then
  echo "[metabase-init] Setup already completed (no setup token). Skipping."
  exit 0
fi

echo "[metabase-init] Running first-time setup with token: $TOKEN"

# Complete setup wizard
SETUP_RESPONSE=$(curl -sf -X POST "$MB_URL/api/setup" \
  -H "Content-Type: application/json" \
  -d "{
    \"token\": \"$TOKEN\",
    \"user\": {
      \"email\": \"$ADMIN_EMAIL\",
      \"password\": \"$ADMIN_PASSWORD\",
      \"first_name\": \"$ADMIN_FIRST\",
      \"last_name\": \"$ADMIN_LAST\",
      \"site_name\": \"GIS Transport MDA\"
    },
    \"prefs\": {
      \"site_name\": \"GIS Transport MDA\",
      \"allow_tracking\": false
    }
  }")

SESSION_ID=$(echo "$SETUP_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)

if [ -z "$SESSION_ID" ]; then
  echo "[metabase-init] Setup failed or already done. Response: $SETUP_RESPONSE"
  exit 0
fi

echo "[metabase-init] Admin account created. Adding PostgreSQL database..."

# Add the transport_db database connection
curl -sf -X POST "$MB_URL/api/database" \
  -H "Content-Type: application/json" \
  -H "X-Metabase-Session: $SESSION_ID" \
  -d '{
    "name": "Transport DB (PostgreSQL)",
    "engine": "postgres",
    "details": {
      "host": "sources",
      "port": 5432,
      "dbname": "transport_db",
      "user": "postgres",
      "password": "postgres",
      "ssl": false
    }
  }' && echo "[metabase-init] Database connected successfully."

echo "[metabase-init] Done. Login at http://localhost:3002"
echo "[metabase-init]   Email:    $ADMIN_EMAIL"
echo "[metabase-init]   Password: $ADMIN_PASSWORD"
