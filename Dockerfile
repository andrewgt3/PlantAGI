# Unified Dockerfile for Python Services
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
# libpq-dev is needed for psycopg2 (even binary sometimes needs libs)
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Set default command (can be overridden in docker-compose)
CMD ["python", "server.py"]
