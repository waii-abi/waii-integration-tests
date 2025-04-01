
* Context:
  - This repository contains integration tests for the WAII system. 
  - Tests are designed to run against dockerized environments. Each test class can specify its desired Docker configuration via custom markers. 
  - Tests are executed in parallel using pytest, while ensuring that tests using the same docker configuration run on the same worker.

* High level details:
  * Docker Configurations:
    - Different docker configurations are available (and can be added) in `tests/docker_launcher/docker_configs.py`. Each command has the following:
      - Fully formatted Docker run command
      - Ready message to confirm container startup. Don't change this, as this is hardcoded in the docker launcher.
      - Startup timeout value.
      - base URL (and optionally, an API key) for initializing the WAII client.
      - **Ensure** to provide different port numbers for different configurations.
  * Fixtures:
    - docker_environment (class‑scoped):
      - Reads the custom @pytest.mark.docker_config marker on a test class (defaults to waii_default), loads the corresponding configuration, cleans up any existing container with the same name, starts the container, and sets environment variables.
    - class_setup_api_client (class‑scoped):
      - Retrieves the base URL and API key from the current Docker configuration and then calls your custom setup and cleanup methods (custom_setup/custom_cleanup) defined on your test class.
  * Parallel Execution:
    - With pytest -n N and the --dist=loadscope option (set in pytest.ini), tests within the same class or module are guaranteed to run on the same worker.
        - This minimizes container conflicts and ensures that a single Docker container is shared for all tests in one class.
  

* Setup:
  - `pip install -r requirements.txt`
  - Review docker configuration in `tests/docker_launcher/docker_configs.py`
    - Notice that MOVIE_DB will not be loaded in this docker. We plan to have local TWEAKIT itself for this. (to reduce time and cost)

* Running Tests:
  - To run specific test (from root folder):
    - `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_basic_postgres_add/test_basic_postgres_add.py`
  - To run all tests:
    - `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html`

* Debugging:
  - We intentionally do not delete the docker containers after the tests are run. In case you need to debug, you can do it. 
  - When tests are started, all containers and its pg/log folders will be deleted.
  - logs about tests are written in `logs` folder.
  - reports are written to `reports` folder.

* FAQ / Yet to fix:
  - I have same docker for multiple test classes. It seems slow.
    - Tests belonging to same docker configs are grouped together and scheduled in the same worker. Within this, it will be executed in sequential mode.
      - It is possible to create additional docker configs with different port numbers and use them in the test classes. This will ensure that the tests are run in parallel.
  - Docker configs are hard to define and maintain.
    - We will try to simlify this in next iteration.
  - Timeout in running all benchmarks together
    - As of now, `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html`
      - Need to improve this. There seems to be an issue in this.
    - Workaround for now, is to run individual tests
      - `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_basic_postgres_add/test_basic_postgres_add.py`
    