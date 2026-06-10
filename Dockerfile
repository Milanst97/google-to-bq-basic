FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run Job: run the script top to bottom, then exit.
CMD ["python", "google_to_bq.py"]
