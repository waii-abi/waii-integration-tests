#!/bin/bash
set -e

usage() {
  echo "Usage: $0 {start|start_and_run|run_tests|stop}"
  echo "  start       - Start Docker container and wait for readiness."
  echo "  start_and_run - Start Docker container and run benchmarks."
  echo "  run_tests       - Run benchmarks only (assumes Docker is running)."
  echo "  stop        - Stop the Docker container."
  exit 1
}

# Cleanup any existing Docker container named "waii"
cleanup_docker_container() {
  echo "Stopping and removing any existing Docker container 'waii'..."
  docker stop waii || true
  docker rm -f waii || true
}

# Set up the base directories and clean their contents
setup_directories() {
  HOME_DIR="$HOME"
  SANDBOX_DIR="$HOME_DIR/waii-sandbox-test-integ"
  PG_DIR="$SANDBOX_DIR/pg"
  LOG_DIR="$SANDBOX_DIR/log"
  RESULTS_DIR="$SANDBOX_DIR/results"

  mkdir -p "$PG_DIR" "$LOG_DIR" "$RESULTS_DIR"
  echo "Cleaning up directories: $PG_DIR, $LOG_DIR, and $RESULTS_DIR"
  rm -rf "$PG_DIR"/*
  rm -rf "$LOG_DIR"/*
}

# Start the Docker container and redirect logs to a file
start_docker() {
  DOCKER_LOG="/tmp/docker_container.log"
  echo "Starting Docker container..."
  docker run --rm \
    --env OPENAI_API_KEY=$OPENAI_API_KEY \
    --env ENABLE_LOG_STREAMING_DOCKER=true \
    -p 3000:3456 \
    -p 9859:9859 \
    -v "$PG_DIR":/var/lib/postgresql/data:rw \
    -v "$LOG_DIR":/tmp/logs:rw \
    --name waii \
    sandbox:latest \
    --debug > "$DOCKER_LOG" 2>&1 &
  DOCKER_PID=$!
  export DOCKER_PID
  export DOCKER_LOG
}

# Wait for the Docker container to print the ready message in its logs
wait_for_docker_ready() {
  READY_MSG="Waii is ready! Please visit http://localhost:3000 to start using it!"
  TIMEOUT=120   # seconds
  INTERVAL=5    # seconds
  elapsed=0

  echo "Waiting for Docker container to be ready..."
  while [ $elapsed -lt $TIMEOUT ]; do
      if grep -q "$READY_MSG" "$DOCKER_LOG"; then
          echo "Ready message detected!"
          return 0
      fi
      sleep $INTERVAL
      elapsed=$((elapsed+INTERVAL))
  done

  echo "Timeout reached. Docker container did not become ready."
  docker stop waii
  exit 1
}

# Run pytest benchmarks (HTML report generation) in parallel
run_tests() {
  echo "Running integration tests..."
  pip install -r ../requirements.txt
  RESULTS_DIR="../results"
  # Generate timestamp with milliseconds; on macOS, you might need to use gdate if GNU date is not available.
  TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S_%3N")
  REPORT_FILE="$RESULTS_DIR/report_${TIMESTAMP}.html"
  echo "Generating report at: $REPORT_FILE"
  pytest -s -n 2 ../tests --html="$REPORT_FILE" --self-contained-html
}

# Stop the Docker container
stop_docker() {
  echo "Stopping Docker container..."
  docker stop waii
  echo "Docker container stopped."
}

# Main script flow
if [ $# -ne 1 ]; then
  usage
fi

MODE="$1"

case "$MODE" in
  start)
    cleanup_docker_container
    setup_directories
    start_docker
    wait_for_docker_ready
    echo "Docker is running. You can now start pytest manually."
    ;;
  start_and_run)
    cleanup_docker_container
    setup_directories
    start_docker
    wait_for_docker_ready
    run_tests
    stop_docker
    ;;
  run_tests)
    echo "Assuming Docker is already running..."
    run_tests
    ;;
  stop)
    stop_docker
    ;;
  *)
    usage
    ;;
esac

echo "Done"
