FROM 172.16.26.144/base/genie-ingestion-base:1.0

WORKDIR /app

COPY . /app
ENV PYTHONPATH=/app

EXPOSE 8000
CMD ["uvicorn", "api.scheduler_service:app", "--host", "0.0.0.0", "--port", "8000"]
