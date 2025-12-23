# Custom Airflow image with pinned base image (same as in docker-compose.yml)
FROM apache/airflow:2.8.1

# Install additional Python dependencies (providers/libs)
# Using --no-cache-dir to keep the image smaller.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

