FROM node:22-alpine
WORKDIR /app
COPY package.json proxy.mjs index.html admin.css admin.js ./
# Key 池落在 /app/data：可挂卷持久化（见 docker-compose.yml），
# 不挂卷时重建容器会丢 Key。
RUN mkdir -p /app/data
ENV CC_KEYS_FILE=/app/data/keys.json
EXPOSE 3050
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --spider http://127.0.0.1:3050/health || exit 1
CMD ["node", "proxy.mjs"]
