#!/bin/bash
# deploy_uat.sh — Deploy UAT (https://uat.expresshealth.ie)
set -e

echo "=== Deploying UAT ==="

# API
cd /home/dev_xpresshealth/uat/uat-xnapi
git pull origin main
docker compose -p uat-xnapi down
docker compose -p uat-xnapi up --build -d

# Admin Panel
cd /home/dev_xpresshealth/uat/uat-xnapi/admin-panel
docker compose -p uat-admin down
docker compose -p uat-admin up --build -d

echo "=== UAT Deployed ==="
docker ps | grep -E "uat-xnapi-api|xh_admin_panel"
