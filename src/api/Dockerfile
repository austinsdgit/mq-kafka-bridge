FROM --platform=linux/amd64 python:3.12-slim

ARG MQ_CLIENT_URL=https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/10.0.1.0-IBM-MQC-Redist-LinuxX64.tar.gz

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates gcc libc-dev && \
    mkdir -p /opt/mqm && \
    curl -fsSL "${MQ_CLIENT_URL}" | tar -xz -C /opt/mqm && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/opt/mqm/lib64:/opt/mqm/lib
ENV MQ_INSTALLATION_PATH=/opt/mqm

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
