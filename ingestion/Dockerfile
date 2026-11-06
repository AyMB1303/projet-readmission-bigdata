# Producteur Kafka : rejoue le dataset UCI comme un flux de dossiers de sortie
FROM python:3.11.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py schema.py ./
ENV PYTHONUNBUFFERED=1
CMD ["python", "producer.py"]
