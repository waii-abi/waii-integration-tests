* Setup
  - Run "chmod 755 bin/start_docker_and_run_tests.sh" 
  - `./start_docker_and_run_tests.sh` to start the docker container and run the tests.
    - Why docker is not added as `fixture` in tests?
      - If we run pytests in parallel, the docker container will be started multiple times. We don't want that.
      - It is much easier to cleanup folders and start docker fresh each time and then run the tests.
    - E.g
    ```
    ./start_docker_and_run_tests.sh
    Usage: ./start_docker_and_run_tests.sh {start|start_and_run|run_tests|stop}
      start       - Start Docker container and wait for readiness.
      start_and_run - Start Docker container and run benchmarks.
      run_tests       - Run benchmarks only (assumes Docker is running).
      stop        - Stop the Docker container.
    ```
  - Results
    - Test results will be generated in report.html in `./reports` folder.

* Creating a test case
  - Chart down out the integration test to be created
  - Create a directory in "tests" folder with the name of the test case
    - `conftest.py` can inject the fixture (api_client) to the test case.
      - E.g function "def test_alias(api_client):"
        - You can activate the DB connection using "api_client.activate_connection" method and proceed further.
  - You can choose to place multiple logical integration tests in the same folder.
    - E.g. "test_alias" and "test_alias_without_sample_values" can be in the same folder.

* Config files for specific tests
  - It is possible that specific test cases may need additional config files. 
  - Since all tests are created in their own directories, place the configs in their respective directories.

* Best practice
  - MOVIE_DB is present by default in the docker container.
  - Ensure to create your own alias connection, instead of using MOVIE_DB.
    - As tests can be run in parallel, this will ensure that the tests are not interfering with each other.

* Skipping a test case
  - You can skip a test case by using the `@pytest.mark.skip` decorator.
  - E.g. 
    ```python
    @pytest.mark.skip(reason="Skipping this test case")
    def test_alias(docker_environment, api_client):
        pass
    ```