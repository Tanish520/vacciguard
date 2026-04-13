FROM python:3.11-slim

WORKDIR /app

# install dependencies first so Docker layer cache is reused on code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]
