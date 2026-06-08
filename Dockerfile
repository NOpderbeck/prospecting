FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts for all Cloud Run jobs
COPY penetration_run.py penetration_publish.py signals_run.py ae_activity_run.py digest_run.py ai_digest_run.py gong_insights_run.py x_content_analysis.py ./

# Copy web server and its dependencies
COPY server.py db.py context.py ask.py ./
COPY templates/ ./templates/
COPY static/ ./static/

CMD ["python3", "server.py", "--host", "0.0.0.0", "--no-reload"]
