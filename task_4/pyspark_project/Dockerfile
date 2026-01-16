FROM python:3.10-slim

USER root
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV JAVA_HOME="/usr/lib/jvm/default-java"

CMD ["python", "main.py"]