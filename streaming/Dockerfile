# kafka-python 2.0.2's vendored compatibility module is incompatible with
# Python 3.12.  The project runtime contract supports Python 3.11+, so the
# container deliberately uses 3.11 until the client is upgraded.
FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python==2.0.2 cassandra-driver==3.29.2 prometheus-client==0.21.1
COPY streaming/ ./streaming/
COPY data-generator/ ./data-generator/
CMD ["python", "streaming/stream_job.py", "--bootstrap-server", "kafka:9092", "--cassandra-host", "cassandra-client", "--metrics-port", "8000"]
