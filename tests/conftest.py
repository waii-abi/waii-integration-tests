import pytest

from tests.docker_configs.docker_configs import DOCKER_CONFIGS, get_pg_dir, get_log_dir, get_base_url
from tests.docker_utils import cleanup_existing_container, start_docker_container, stop_docker_container
from tests.log_util import init_logger
from tests.utils import init_api_client


logger = init_logger()

@pytest.fixture(scope="class")
def docker_environment(request):
    """
    Class-scoped fixture that:
      - Reads the 'docker_config' marker from the test class (defaults to 'waii_default').
      - Loads the corresponding configuration from DOCKER_CONFIGS.
      - Starts the Docker container using the fully formatted run_command.
      - Yields control for the test class, then stops the container on teardown.
    """

    # marker = request.node.get_closest_marker("docker_config")
    # logger.info(f"docker_config marker: {marker}")
    # config_key = marker.args[0] if marker else "waii_default"
    # config = DOCKER_CONFIGS.get(config_key)
    config, docker_name = get_config_for_docker(request)


    # Use the fully formatted run_command and ready_message from the configuration.
    ready_message = config["ready_message"]
    startup_timeout = config.get("startup_timeout", 120)

    # Assume the container name is the same as the config key.
    container_name = docker_name

    # Replace container name in the placeholders. i.e {container_name}, {pg_dir_container_name}, {log_dir_container_name}
    api_port = str(config.get("api_port", 9859))
    run_command = (config["run_command"].replace("{{pg_dir_container_name}}", get_pg_dir(container_name))
                   .replace("{{log_dir_container_name}}", get_log_dir(container_name))
                   .replace("{{port}}", api_port)
                   .replace("{{container_name}}", container_name))

    logger.info(f"launching docker name: {container_name}: command {run_command}")

    # Ensure the container is not running by cleaning up any existing instance.
    cleanup_existing_container(container_name)

    logger.info(f"Starting Docker container with configuration: {container_name}")
    proc = start_docker_container(run_command, ready_message, startup_timeout, container_name)

    yield  # Tests in the class execute here.

    # TODO: If you don't want want docker container to stop, comment out the following for debugging
    print(f"Stopping Docker container with configuration: {container_name}")
    stop_docker_container(container_name)

def get_config_for_docker(request):
    marker = request.node.get_closest_marker("docker_config")
    logger.info(f"docker_config marker in class_setup_api_client: {marker}")
    docker_name = marker.args[0] if marker else "waii_default"
    config = DOCKER_CONFIGS.get(docker_name)
    if not config:
        pytest.fail(f"No Docker configuration found for key: {docker_name}")
    return config, docker_name

@pytest.fixture(scope="class", autouse=True)
def class_setup_api_client(request, docker_environment):
    """
    This fixture is automatically applied to all test classes.
    It initializes API_CLIENT and passes it to custom_setup() and custom_cleanup() methods of the test class.
    """
    config, docker_name = get_config_for_docker(request)
    base_url = get_base_url(config)
    api_key = config.get("api_key")
    cls = request.cls
    logger.info(f"Starting API client with configuration: url: {base_url}, api_key: {api_key} for docker: {docker_name}")
    api_client = init_api_client(base_url=base_url, api_key=api_key)
    if hasattr(cls, "custom_setup"):
        logger.info(f"Running custom setup() for {cls.__name__} with base_url: {base_url} and api_key: {api_key}")
        cls.custom_setup(api_client=api_client)
    yield
    if hasattr(cls, "custom_cleanup"):
        logger.info(f"Running custom cleanup() for {cls.__name__} with base_url: {base_url} and api_key: {api_key}")
        cls.custom_cleanup(api_client=api_client)
