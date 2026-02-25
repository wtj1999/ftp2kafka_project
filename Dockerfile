# ===== Dockerfile =====
FROM python:3.11-slim

# 切回 root 并设置工作目录
USER root
WORKDIR /app

RUN echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian bookworm main contrib non-free" > /etc/apt/sources.list && \
    echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian bookworm-updates main contrib non-free" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.tuna.tsinghua.edu.cn/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    wget \
    gcc \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN mkdir -p /etc && \
    printf '[global]\nindex-url = https://pypi.tuna.tsinghua.edu.cn/simple\ntrusted-host = pypi.tuna.tsinghua.edu.cn\ntimeout = 120\n' > /etc/pip.conf
RUN pip install --no-cache-dir -r /app/requirements.txt --timeout 120 --retries 6

COPY . /app

EXPOSE 8000

#VOLUME ["/data"]

CMD ["python", "-u", "poller.py"]
