# CFData 优选 Web 管理平台
# 镜像: python:3.11-slim (cfdata-linux-amd64 仅支持 amd64)
FROM python:3.11-slim

LABEL org.opencontainers.image.title="cfdata-web-manager" \
      org.opencontainers.image.description="CFData 优选 Web 管理平台：多源管理/cron定时/Top节点双格式输出" \
      org.opencontainers.image.source="https://github.com/totootao/cfdata-web-manager"

WORKDIR /app

# 安装 ca-certificates 供 cfdata 访问 https 测速源
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app/results /app/web

COPY app.py /app/app.py
COPY web/ /app/web/
COPY cfdata-linux-amd64 /app/cfdata-linux-amd64

RUN chmod +x /app/cfdata-linux-amd64

# 配置与结果持久化目录
VOLUME ["/app/results", "/app/data"]

ENV PYTHONUNBUFFERED=1

EXPOSE 8088

# 健康检查: 应用根路径
HEALTHCHECK --interval=60s --timeout=8s --start-period=20s --retries=3 \
    CMD python3 -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8088/', timeout=6)" || exit 1

ENTRYPOINT ["python3", "/app/app.py"]
CMD ["--host", "0.0.0.0", "--port", "8088"]
