import os
import shutil
import time
import subprocess
import threading
import pytest
from waii_sdk_py import WAII, Waii

from tests.docker_launcher.docker_configs import DOCKER_CONFIGS
from tests.log_util import init_logger
from tests.utils import init_api_client

# Global variable to store the currently selected Docker configuration.
CURRENT_DOCKER_CONFIG = None

logger = init_logger()

def start_docker_container(run_command, ready_message, startup_timeout):
    """Starts a Docker container using the provided run_command and waits until the ready_message is detected."""
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
            logger.info("Ready message detected!")
            return proc
        time.sleep(0.5)
    proc.terminate()
    raise TimeoutError("Docker container did not become ready within the timeout period.")

def cleanup_existing_container(container_name):
    """Stop and remove any existing Docker container with the given name."""
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
    subprocess.run(["docker", "stop", container_name], check=True)
    logger.info(f"Docker container '{container_name}' stopped.")

@pytest.fixture(scope="class")
def docker_environment(request):
    """
    Class-scoped fixture that:
      - Reads the 'docker_config' marker from the test class (defaults to 'waii_default').
      - Loads the corresponding configuration from DOCKER_CONFIGS.
      - Starts the Docker container using the fully formatted run_command.
      - Yields control for the test class, then stops the container on teardown.
    """
    global CURRENT_DOCKER_CONFIG

    marker = request.node.get_closest_marker("docker_config")
    config_key = marker.args[0] if marker else "waii_default"
    config = DOCKER_CONFIGS.get(config_key)
    if not config:
        pytest.fail(f"No Docker configuration found for key: {config_key}")

    CURRENT_DOCKER_CONFIG = config

    # Use the fully formatted run_command and ready_message from the configuration.
    run_command = config["run_command"]
    ready_message = config["ready_message"]
    startup_timeout = config.get("startup_timeout", 120)

    # Assume the container name is the same as the config key.
    container_name = config_key

    # Ensure the container is not running by cleaning up any existing instance.
    cleanup_existing_container(container_name)

    logger.info(f"Starting Docker container with configuration: {config_key}")
    proc = start_docker_container(run_command, ready_message, startup_timeout)

    yield  # Tests in the class execute here.

    # TODO: Intentionally not stopping container for now.
    #print(f"Stopping Docker container with configuration: {config_key}")
    #stop_docker_container(container_name)

@pytest.fixture(scope="class", autouse=True)
def class_setup_api_client(request, docker_environment):
    """
    This fixture is automatically applied to all test classes.
    It initializes API_CLIENT and passes it to custom_setup() and custom_cleanup() methods of the test class.
    """
    global CURRENT_DOCKER_CONFIG
    base_url = CURRENT_DOCKER_CONFIG.get("base_url")
    api_key = CURRENT_DOCKER_CONFIG.get("api_key")
    cls = request.cls
    api_client = init_api_client(base_url=base_url, api_key=api_key)
    if hasattr(cls, "custom_setup"):
        logger.info(f"Running custom setup() for {cls.__name__} with base_url: {base_url} and api_key: {api_key}")
        cls.custom_setup(api_client=api_client)
    yield
    if hasattr(cls, "custom_cleanup"):
        logger.info(f"Running custom cleanup() for {cls.__name__} with base_url: {base_url} and api_key: {api_key}")
        cls.custom_cleanup(api_client=api_client)
