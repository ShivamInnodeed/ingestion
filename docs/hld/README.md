# Website Ingestion — HLD for Support KT

PNG diagrams for knowledge transfer to support engineers.

| Slide | File | Use in KT |
|-------|------|-----------|
| 1 | [hld-01-system-context.png](./hld-01-system-context.png) | What systems talk to what |
| 2 | [hld-02-e2e-pipeline.png](./hld-02-e2e-pipeline.png) | 9 LangGraph stages end-to-end |
| 3 | [hld-03-ops-run-lifecycle.png](./hld-03-ops-run-lifecycle.png) | How `/run-now` works + how to debug |
| 4 | [hld-04-crawl-scrapers.png](./hld-04-crawl-scrapers.png) | Why runs get “stuck” (offers scraper) |

Editable Mermaid sources: `*.mmd` in this folder (paste into https://mermaid.live to re-export).

---

## KT talking points (5–7 minutes)

1. **Purpose** — Crawl card website pages → detect content changes → chunk/embed → index into Elasticsearch for search/RAG.
2. **Entry point** — Podman container `scheduler-api-seed-8004` on port **8005**. API does not crawl itself; it starts `python langgraph_ingestion_flow.py`.
3. **Pipeline** — 9 nodes: crawl → audit_raw → llm_enrich → apply_llm → build_payloads → delta_select → build_embeddings → sync_elasticsearch → finalize_run.
4. **Heavy optional stage** — `scrape_offers=true` runs Playwright over hundreds of offer pages; this is the usual cause of long/failed runs.
5. **Artifacts** — Host mount `/App1/langgraph/runtime/output_clean_1/` (logs, JSON, embeddings). Temp: `/App1/langgraph/runtime/tmp` when `TMPDIR=/app/tmp` is set.
6. **Debug order** — `/scheduler/status` → `podman exec … ps` → `tail` run log → `podman inspect … OOMKilled` → check tmp disk.
7. **Stale run** — PID gone but `runs.running:true` → restart container to clear in-memory state. `/scheduler/stop` only stops the schedule, not an active job.

---

## Key APIs

```bash
# Health
curl -s "http://HOST:8005/health"

# Status (is a run active?)
curl -s "http://HOST:8005/scheduler/status" -H "x-api-key: changeme"

# Start run (example: no offers)
curl -X POST "http://HOST:8005/run-now" \
  -H "x-api-key: changeme" -H "Content-Type: application/json" \
  -d '{"scrape_offers":false,"max_depth":4,"max_pages":800}'
```

---

## Key code

| Component | Path |
|-----------|------|
| Scheduler API | `api/scheduler_service.py` |
| LangGraph pipeline | `langgraph_ingestion_flow.py` |
| Main crawl | `crawl_sbicard_markdown.py` |
| Offer scraper | `offer/sbi_offer_scraper.py` |
| ES + audit DB | `es_kb/` |
