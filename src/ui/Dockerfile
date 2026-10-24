FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

# nginx-unprivileged runs on port 8080 as non-root — safe for OpenShift
FROM nginxinc/nginx-unprivileged:alpine AS nginx
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 8080
CMD ["nginx", "-g", "daemon off;"]
