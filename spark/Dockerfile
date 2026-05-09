# syntax=docker/dockerfile:1.6
FROM bitnami/spark:3.5.0

USER root

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /opt/spark

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY conf/ /opt/bitnami/spark/conf/
COPY jobs/ /opt/spark/jobs/

# Pre-create the checkpoint dir so the streaming sink can write without root perms.
RUN mkdir -p /opt/spark/checkpoints \
 && chmod -R 777 /opt/spark/checkpoints

USER 1001

CMD ["bash", "-c", "echo 'spark image ready — call with spark-submit'"]
