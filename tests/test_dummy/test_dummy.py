import time

import pytest

from tests.log_util import init_logger

"""
- Test suite just for demoing that multiple dockers can be launched in parallel and tests can be run within that.
- This will launch 3 dockers in parallel and run the tests in each of them.
- Each test will run for 10 seconds; They have their own setup and cleanup.
- Check README.md to see how to run this.
"""
# Init the logger for this class
logger = init_logger(log_file="logs/test_dummy.log")


@pytest.mark.docker_config("waii_default")
class Test_docker_waii_default:
    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"
        cls.api_client = api_client

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = None
        cls.api_client = api_client

    def test_feature_one(self, docker_environment):
        logger.info("Running Test_docker_waii_default.feature_one;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None

    def test_feature_two(self, docker_environment):
        logger.info("Running Test_docker_waii_default.test_feature_two;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None


@pytest.mark.docker_config("waii_default_ex_1")
class Test_docker_waii_default_ex_1:
    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"
        cls.api_client = api_client

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = None
        cls.api_client = api_client

    def test_feature_one(self, docker_environment):
        logger.info("Running Test_docker_waii_default_ex_1.test_feature_one;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None

    def test_feature_two(self, docker_environment):
        logger.info("Running Test_docker_waii_default_ex_1.test_feature_two;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None


@pytest.mark.docker_config("waii_default_ex_2")
class Test_docker_waii_default_ex_2:
    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"
        cls.api_client = api_client

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = None
        cls.api_client = api_client

    def test_feature_one(self, docker_environment):
        logger.info("Running Test_docker_waii_default_ex_2.test_feature_one;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None

    def test_feature_two(self, docker_environment):
        logger.info("Running Test_docker_waii_default_ex_2.test_feature_two;")
        time.sleep(5)
        api_client = self.api_client
        assert api_client is not None
