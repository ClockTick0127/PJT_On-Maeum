# syntax=docker/dockerfile:1
FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
ARG FRONTEND_ENV_HASH=none
RUN --mount=type=secret,id=frontend_env,target=/app/.env.production test -n "$FRONTEND_ENV_HASH" && npm run lint && npm run build

FROM nginx:1.28-alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
HEALTHCHECK --interval=10s --timeout=3s CMD wget -qO- http://127.0.0.1/healthz || exit 1
