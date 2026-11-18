FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY management /app/management
COPY dnsnode /app/dnsnode

EXPOSE 8000
CMD ["uvicorn", "management.app:app", "--host", "0.0.0.0", "--port", "8000"]
