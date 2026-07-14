# Offline Elasticsearch Bundle Replay

This folder is copied into each generated offline bundle. Use it to replay the exact indexing payload on an air-gapped server.

## Bundle Contents

- `bundle_manifest.json`
- `es_effective_config.json`
- `embedding_vectors.jsonl`
- `deleted_doc_ids.jsonl`
- `offline_index_to_es.py`

## Server Run Steps

1. Extract bundle zip on the server.
2. Ensure Python 3.10+ is installed (no extra pip dependencies required).
3. Run:

```bash
python offline_index_to_es.py --bundle-dir . --es-url http://localhost:9200 --recreate-index
```

If Elasticsearch needs auth:

```bash
python offline_index_to_es.py --bundle-dir . --es-url http://localhost:9200 --es-username elastic --es-password 'your-password' --recreate-index
```

## Reindex After Index Deletion

If index is deleted from Kibana, rerun the same command with `--recreate-index` and the bundle data to rebuild the same index contents.
