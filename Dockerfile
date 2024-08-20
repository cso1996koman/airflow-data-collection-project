FROM apache/airflow:2.7.1-python3.8

# Install necessary packages
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libkrb5-dev \
        vim \
	&& apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user and install Python dependencies
USER airflow
RUN pip install --no-cache-dir joblib==1.2.0 pycaret==3.2.0 pandas scikit-learn requests apache-airflow[hdfs] apache-airflow[hive]