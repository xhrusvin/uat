# Xpress Health Admin Panel

React + Tailwind admin panel for the XpressHealth User API.

## Features

- JWT login (admin users only) with instant logout bug fixed
- Split login page with XpressHealth branding
- Dashboard with user stats
- Users list with search, pagination, per-page control
- User detail drawer
- Auto logout on token expiry
- Responsive layout

## Local development

```bash
cd admin-panel
npm install
npm run dev
# → http://localhost:8051
```

Make sure FastAPI is running on port 8050:
```bash
python -m uvicorn app.main:app --reload --port 8050
```

## Production build

```bash
npm run build
# Output: admin-panel/dist/
```

## Deploy to server

```bash
cd /home/dev_xpresshealth/uat/uat-xnapi/admin-panel
npm install && npm run build

# Add nginx.conf.snippet to your Nginx config
sudo nginx -t && sudo systemctl reload nginx
# → https://uat.expresshealth.ie/xnadmin/
```
