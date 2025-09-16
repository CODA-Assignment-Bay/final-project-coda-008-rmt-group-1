FROM apache/airflow:2.3.4

# Install Java (OpenJDK 8 or 11 depending on your preference)
USER root
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV CLASSPATH=/opt/airflow/jars/*

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt