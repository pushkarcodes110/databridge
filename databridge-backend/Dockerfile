FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2 and pandas
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Ensure upload directory exists
RUN mkdir -p uploads

# Expose API port
EXPOSE 8000
