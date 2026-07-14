This folder is for offline Python wheels that must be bundled into the container image
for air-gapped execution.

## Embedding model (recommended for server)

Place these files here before building the image tar for the server:

- `embedding_service-*.whl` (your internal wheel that provides `create_embedding_service()`)

Then rebuild the image. The Dockerfile will install whatever wheels are present in this
folder.

