FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Cloud Run configuration
ENV OASIS_API_TOKEN=6df8fe289b263ef4e221123e5f21bd83f27eca44
ENV OASIS_NETWORK_URL=https://ipm.oasisinsight.net
ENV DB_HOST=""

# Cloud Run sets PORT env var (default 8080)
EXPOSE 8080

# Start Flask app — gunicorn will use PORT env var if set
CMD ["sh", "-c", "gunicorn -w 1 -b 0.0.0.0:${PORT:-8080} --timeout=120 server:app"]
