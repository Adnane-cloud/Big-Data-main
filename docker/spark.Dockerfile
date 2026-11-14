# Image Spark 3.5 (Scala 2.12, compatible avec le connecteur MongoDB 10.4) + numpy (requis par MLlib/ALS en Python)
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-numpy && rm -rf /var/lib/apt/lists/*
USER spark
