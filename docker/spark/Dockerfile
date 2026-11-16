# Spark 3.5.9 (Java 17 + Python 3) + JupyterLab.
# Meme image pour spark-master, spark-worker et jupyter (meme version Python partout).
FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu
USER root
ENV PIP_BREAK_SYSTEM_PACKAGES=1 PIP_NO_CACHE_DIR=1
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps \
 && rm -rf /var/lib/apt/lists/* \
 && pip3 install jupyterlab pandas numpy matplotlib pyarrow
USER spark
