FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts for all Cloud Run jobs
COPY penetration_run.py penetration_publish.py signals_run.py ae_activity_run.py digest_run.py ai_digest_run.py ./

CMD ["python3", "penetration_run.py"]
