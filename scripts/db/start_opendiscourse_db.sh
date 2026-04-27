#!/usr/bin/env bash
set -euo pipefail

# Image is pinned to version used during the analysis
IMAGE="ghcr.io/open-discourse/open-discourse/database@sha256:850c7841a8570237f340208ce359d367bd747d6ef4441cc5df2ea3bea1bad7ac"
CONTAINER_NAME="open-discourse-db"

# Check Docker is available
if ! command -v docker &>/dev/null; then
  echo "Error: Docker is not installed or not in PATH." >&2
  exit 1
fi

if ! docker info &>/dev/null; then
  echo "Error: Docker daemon is not running." >&2
  exit 1
fi

# Check for an existing container with this image (running or stopped)
existing=$(docker ps -a --filter "ancestor=$IMAGE" --format "{{.Names}}" 2>/dev/null)
if [ -n "$existing" ]; then
  running=$(docker ps --filter "ancestor=$IMAGE" --format "{{.Names}}" 2>/dev/null)
  if [ -n "$running" ]; then
    echo "Container '$existing' is already running."
  else
    echo "Starting existing container '$existing' ..."
    docker start "$existing"
  fi
else
  # Pull the image, capturing output to detect auth failures
  echo "Pulling $IMAGE ..."
  pull_output=$(docker pull "$IMAGE" 2>&1) || {
    if echo "$pull_output" | grep -qiE "unauthorized|denied|authentication required"; then
      echo "Error: Not authorized to pull from ghcr.io." >&2
      echo "Authenticate with: echo <TOKEN> | docker login ghcr.io -u <USERNAME> --password-stdin" >&2
    else
      echo "Error: Failed to pull image." >&2
      echo "$pull_output" >&2
    fi
    exit 1
  }

  echo "Starting container '$CONTAINER_NAME' ..."
  docker run \
    --name "$CONTAINER_NAME" \
    --env POSTGRES_USER=postgres \
    --env POSTGRES_DB=postgres \
    --env POSTGRES_PASSWORD=postgres \
    -p 5432:5432 \
    -d \
    "$IMAGE"
fi

echo "Database running at localhost:5432"
echo "  host:     localhost"
echo "  port:     5432"
echo "  database: next"
echo "  user:     postgres"
echo "  password: postgres"
