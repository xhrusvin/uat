#!/bin/bash
# deploy_prod.sh — Deploy Prod (https://expresshealth.ie)
set -e

echo "=== Deploying Prod ==="

# API
cd /home/dev_xpresshealth/recruitassist_project/uat-xnapi
git pull origin main
docker compose -p prod-xnapi down
docker compose -p prod-xnapi up --build -d

# Admin Panel
cd /home/dev_xpresshealth/recruitassist_project/uat-xnapi/admin-panel
docker compose -p prod-admin down
docker compose -p prod-admin up --build -d

echo "=== Prod Deployed ==="
docker ps | grep -E "prod.*api|prod_admin_panel"
