# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json ./
RUN npm install

COPY . .

# Build with production env
RUN npm run build


# ── Stage 2: Serve with Nginx ─────────────────────────────────────────────────
FROM nginx:alpine AS runtime

# Install envsubst (part of gettext)
RUN apk add --no-cache gettext

# Copy built files
COPY --from=builder /app/dist /usr/share/nginx/html/xnadmin

# Copy nginx template
COPY nginx.docker.conf /etc/nginx/templates/default.conf.template

# Default port — override with ADMIN_PORT env var
ENV ADMIN_PORT=8051

EXPOSE ${ADMIN_PORT}

# envsubst replaces ${ADMIN_PORT} in the template at container startup
CMD ["/bin/sh", "-c", \
  "envsubst '${ADMIN_PORT}' < /etc/nginx/templates/default.conf.template \
   > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]
