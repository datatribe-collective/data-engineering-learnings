# Based on the official Airflow image
FROM apache/airflow:2.8.1

USER root

# Install system packages as root (JDK for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory
WORKDIR /opt/airflow

# Switch to airflow user to install Python packages
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy project files into image
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
COPY config /opt/airflow/config