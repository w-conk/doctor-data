FROM apache/airflow:2.8.0-python3.11

# Switch to root to install packages
USER root

# Install system dependencies if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file (from project root, since build context is .)
COPY --chown=airflow:root requirements.txt /requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r /requirements.txt
