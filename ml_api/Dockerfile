FROM python:3.10-slim
RUN apt-get update && apt-get install -y --no-install-recommends default-jdk-headless curl \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fastapi_serving.py .
EXPOSE 8000
CMD ["python", "fastapi_serving.py"]
