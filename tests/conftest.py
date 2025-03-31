import os

import pytest
import yaml
from waii_sdk_py import Waii

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../config/integration_config.yaml")


def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def api_client() -> Waii:
    """
    Function-scoped fixture that creates a new WAII SDK client instance
    for each test. This ensures tests do not share the same client.
    """
    config = load_config()
    env = config["environment"]
    client = Waii()
    client.initialize(url=env["base_url"], api_key=env["api_key"])
    # client.database.activate_connection("default")
    print("Initialized WAII client with base URL:", env["base_url"])
    return client
