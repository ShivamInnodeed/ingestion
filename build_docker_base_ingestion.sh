#!/usr/bin/env bash

# Build the Genie Data-Ingestion base Docker image.
# Usage: ./build_docker_base_ingestion.sh <version>
# Example: ./build_docker_base_ingestion.sh 1.0
# Result: genie-ingestion-base:1.0

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <version>"
  echo "Example: $0 1.0"
  exit 1
fi

VERSION="$1"
IMAGE_NAME="genie-ingestion-base"
IMAGE_REF="${IMAGE_NAME}:${VERSION}"
ARCHIVE_NAME="${IMAGE_NAME}-${VERSION}.tar.gz"

docker build --no-cache --pull --platform linux/amd64 \
  -f "${SCRIPT_DIR}/Dockerfile_base_ingestion" \
  -t "${IMAGE_REF}" \
  "${SCRIPT_DIR}"
docker save "${IMAGE_REF}" | gzip > "${SCRIPT_DIR}/${ARCHIVE_NAME}"

echo "Built image: ${IMAGE_REF}"
echo "Saved archive: ${SCRIPT_DIR}/${ARCHIVE_NAME}"
