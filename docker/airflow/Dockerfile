FROM apache/airflow:2.10.4

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir requests boto3 minio "apache-airflow-providers-apache-spark==4.10.0" "pyspark==3.5.1"
