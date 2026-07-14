# Postman-Triggered LangGraph Ingestion

This stack runs LangGraph ingestion through a Scheduler API (FastAPI), triggered from Postman, with Elasticsearch `9.2.1` and Kibana `9.2.1` in Docker.

## Stack components

- `scheduler-api` (FastAPI + in-process scheduler)
- Elasticsearch `9.2.1`
- Kibana `9.2.1`

## Prerequisites

- Docker Desktop with WSL integration enabled (if using WSL)
- Minimum 4-6 GB RAM available for Docker

## Start locally

From repository root:

```bash
cd /mnt/d/ingestion
export SCHEDULER_API_KEY='your-strong-key'
docker compose up -d --build
```

Services:

- Scheduler API: `http://localhost:8000`
- Elasticsearch: `http://localhost:9200`
- Kibana: `http://localhost:5601`

## API endpoints (for Postman)

Use header on protected endpoints:

- `x-api-key: your-strong-key`

### Health

```http
GET /health
```

### Run ingestion now

```http
POST /run-now
```

Sample body:

```json
{
  "max_depth": 3,
  "max_pages": 150,
  "crawl_output_dir": "/app/output_clean_1",
  "payload_output": "/app/output_clean_1/langgraph_embedding_payloads.jsonl",
  "embeddings_output": "/app/output_clean_1/langgraph_embedding_vectors.jsonl",
  "state_output": "/app/output_clean_1/langgraph_ingestion_state.json",
  "offline_bundle_dir": "/app/output_clean_1/offline_es_bundle",
  "generate_offline_bundle": true,
  "config_path": "/app/es_search_config.json",
  "es_url": "http://elasticsearch:9200",
  "retry_failed_pages": true
}
```

### Start recurring scheduler (interval)

```http
POST /scheduler/start
```

Sample body:

```json
{
  "interval_seconds": 3600,
  "run_options": {
    "max_depth": 3,
    "max_pages": 150,
    "es_url": "http://elasticsearch:9200",
    "crawl_output_dir": "/app/output_clean_1"
  }
}
```

### Start recurring scheduler (cron)

```http
POST /scheduler/start
```

Sample body:

```json
{
  "cron": "0 */6 * * *",
  "run_options": {
    "max_depth": 3,
    "max_pages": 150,
    "es_url": "http://elasticsearch:9200"
  }
}
```

### Stop scheduler

```http
POST /scheduler/stop
```

### Scheduler and run status

```http
GET /scheduler/status
```

## Logs and checks

Service status:

```bash
docker compose ps
```

Scheduler API logs:

```bash
docker compose logs -f scheduler-api
```

Elasticsearch health:

```bash
curl http://localhost:9200/_cluster/health
```

## Offline bundle for air-gapped server replay

Each `/run-now` execution also exports an offline replay bundle (default path: `output_clean_1/offline_es_bundle`) containing:

- `embedding_vectors.jsonl`
- `deleted_doc_ids.jsonl`
- `es_effective_config.json`
- `bundle_manifest.json`
- `offline_index_to_es.py`
- `README_OFFLINE.md`

Zip the bundle folder and move it to the air-gapped server. On server:

```bash
cd offline_es_bundle
python offline_index_to_es.py --bundle-dir . --es-url http://localhost:9200 --recreate-index
```

The replay script is standalone and uses Python stdlib only (no extra pip packages required on server).

If the index is deleted from Kibana and data has changed, generate a fresh bundle locally and rerun the same command on server with the new bundle.

## Image export/import for server handoff

Build and export scheduler image:

```bash
docker compose build scheduler-api
docker save -o langgraph-scheduler-api-latest.tar langgraph-scheduler-api:latest
```

Load on target server:

```bash
docker load -i langgraph-scheduler-api-latest.tar
docker compose up -d
```

## Stop stack

```bash
docker compose down
```
