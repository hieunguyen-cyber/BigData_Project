FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir Flask==3.0.3 prometheus-client==0.21.1 cassandra-driver==3.29.2
COPY app.py server.py ./
EXPOSE 8080
CMD ["python", "server.py"]
