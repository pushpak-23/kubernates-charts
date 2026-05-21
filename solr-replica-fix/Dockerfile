FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY solr_collection_watcher.py /app/solr_collection_watcher.py

CMD ["python", "/app/solr_collection_watcher.py"]