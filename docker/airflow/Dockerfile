FROM apache/airflow:3.3.1-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    pyspark==4.0.4

USER root

RUN curl -fL \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
    -o /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/hadoop-aws-3.4.1.jar \
    && curl -fL \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.6/bundle-2.24.6.jar \
    -o /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/bundle-2.24.6.jar

USER airflow