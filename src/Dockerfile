# Use the official Spark PySpark image as the base image
FROM apache/spark:3.4.2-scala2.12-java11-ubuntu

# Switch to the root user for installing additional dependencies
USER root

# Install Python 3 and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and setuptools
RUN pip3 install --upgrade pip setuptools --user

# Clean up unnecessary files and directories
RUN rm -r /root/.cache && rm -rf /var/cache/apt/*

# Set Python version for PySpark
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

# Set the working directory
WORKDIR /opt/application

# Copy requirements and PySpark ETL script
COPY requirements.txt .
COPY ETL/etl_pipeline.py /opt/application/etl_pipeline.py
COPY mysql-connector-java-8.0.30.jar /opt/spark/jars
COPY entrypoint.sh /entrypoint.sh

# Install Python dependencies
RUN pip3 install -r requirements.txt --user

# # Switch back to the spark user
# USER spark

# Set the entry point
ENTRYPOINT ["sh", "/entrypoint.sh"]
