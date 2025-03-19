FROM apache/airflow:2.10.5

USER root

RUN apt-get update && \
    apt-get install -y python3-pip default-jdk ant && \
    apt-get install -y curl wget unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"

USER airflow

RUN pip install --no-cache-dir pyspark
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

