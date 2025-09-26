# Dockerfile
FROM python:3.11-slim

# 避免 Python 输出被缓冲（有利于日志）
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

# 安装系统依赖（如果 confluent-kafka 需要 librdkafka）
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    libsasl2-dev \
    libsasl2-modules-gssapi-mit \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# 安装 librdkafka（confluent-kafka 的依赖）
RUN apt-get update && apt-get install -y --no-install-recommends librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制代码
COPY . /app

# 安装 python 依赖
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# 运行目录持久化数据
VOLUME ["/data"]

# 默认命令运行你的 runner（轮询 FTP 的主循环）
CMD ["python", "-u", "poller.py"]
