# Xpress Health Admin Panel

React + Tailwind admin panel — served at `https://uat.expresshealth.ie/xnadmin/`

---

## Local development

```bash
cd admin-panel
npm install
npm run dev
# → http://localhost:8051
```

Backend must be running on port 8050:
```bash
python -m uvicorn app.main:app --reload --port 8050
```

---

## Deploy to Debian server (Docker)

### Step 1 — Pull latest code
```bash
cd /home/dev_xpresshealth/uat/uat-xnapi
git pull
```

### Step 2 — Build and start the container
```bash
cd /home/dev_xpresshealth/uat/uat-xnapi/admin-panel
docker compose up --build -d
```

### Step 3 — Add Nginx proxy block
```bash
sudo nano /etc/nginx/sites-enabled/default   # or your config file
```

Add inside the `server { listen 443 ... }` block:
```nginx
location /xnadmin/ {
    proxy_pass         http://127.0.0.1:8051/xnadmin/;
    proxy_http_version 1.1;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;
}
```

```bash
sudo nginx -t && sudo systemctl reload nginx
```

### Step 4 — Test
```bash
curl https://uat.expresshealth.ie/xnadmin/
```

---

## Redeploy after code changes

```bash
cd /home/dev_xpresshealth/uat/uat-xnapi
git pull
cd admin-panel
docker compose down
docker compose up --build -d
docker compose logs -f admin
```

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `VITE_API_URL` | FastAPI base URL (set per env file) |
| `VITE_API_KEY` | API key for `/users/` endpoints |

- `.env.local` — local dev (Vite proxy to localhost:8050)
- `.env.production` — production build (UAT server URL)
