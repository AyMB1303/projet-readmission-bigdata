# Image Spark du cluster : image officielle Apache + dépendances Python du projet
ARG SPARK_IMAGE=apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

USER root
# numpy est requis par pyspark.ml ; curl sert aux healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && (pip3 install --no-cache-dir numpy==1.26.4 pandas==2.2.3 \
     || pip3 install --no-cache-dir --break-system-packages numpy==1.26.4 pandas==2.2.3) \
 && mkdir -p /tmp/spark-work /tmp/.ivy2 /opt/app \
 && chown -R spark:spark /tmp/spark-work /tmp/.ivy2 /opt/app

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PYSPARK_PYTHON=python3 \
    HADOOP_USER_NAME=spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH
USER spark
WORKDIR /opt/app
