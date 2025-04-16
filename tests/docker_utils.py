import os
import shutil
import subprocess
import threading
import time

from tests.log_util import init_logger

logger = init_logger()

def start_docker_container(run_command, ready_message, startup_timeout, container_name):
    """Starts a Docker container using the provided run_command and waits until the ready_message is detected."""
    if str(container_name).endswith("_local"):
        logger.info("start_docker_container: local process")
        return None
    proc = subprocess.Popen(
        run_command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,     # line-buffered
        text=True      # enable text mode
    )
    ready = False
    start_time = time.time()

    def read_output():
        nonlocal ready
        for line in iter(proc.stdout.readline, ''):
            print(line, end='', flush=True)
            if ready_message in line:
                ready = True
                break

    reader_thread = threading.Thread(target=read_output, daemon=True)
    reader_thread.start()

    while time.time() - start_time < startup_timeout:
        if ready:
            logger.info(f"Ready message detected! for docker: {container_name}")
            return proc
        time.sleep(2)
        logger.info(f"Waiting for Docker container to be ready... {time.time() - start_time:.2f}s elapsed")
    proc.terminate()
    raise TimeoutError("Docker container did not become ready within the timeout period.")

def cleanup_existing_container(container_name):
    """Stop and remove any existing Docker container with the given name."""
    if str(container_name).endswith("_local"):
        logger.info("cleanup_existing_container: local process")
        return
    logger.info(f"Cleaning up any existing Docker container '{container_name}'...")
    subprocess.run(["docker", "stop", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


    config_pg_dir = os.path.join(os.environ.get("HOME"), "waii-sandbox-test-integ", "pg", container_name)
    config_log_dir = os.path.join(os.environ.get("HOME"), "waii-sandbox-test-integ", "log", container_name)
    logger.info(f"Cleaning up directories: pg:{config_pg_dir}, log: {config_log_dir}")
    shutil.rmtree(config_pg_dir, ignore_errors=True)
    shutil.rmtree(config_log_dir, ignore_errors=True)


def stop_docker_container(container_name):
    """Stops the Docker container with the specified name."""
    if str(container_name).endswith("_local"):
        logger.info("stop_docker_container: local process")
        return
    subprocess.run(["docker", "stop", container_name], check=True)
    logger.info(f"Docker container '{container_name}' stopped.")

