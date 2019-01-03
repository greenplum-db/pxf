ARG BASE_IMAGE=gpdb-pxf-dev:centos6-hdp-server

FROM pivotaldata/${BASE_IMAGE}

RUN useradd -s /sbin/nologin -d /opt/minio minio && \
    mkdir -p /opt/minio/bin && \
    chmod a+rx /opt/minio && \
    mkdir /opt/minio/data && \
    wget https://dl.minio.io/server/minio/release/linux-amd64/minio -O /opt/minio/bin/minio && \
    chmod +x /opt/minio/bin/minio && \
    chown -R minio:minio /opt/minio
