import pytest
from waii_sdk_py.database import ModifyDBConnectionRequest, DBConnection, DBContentFilter, DBContentFilterScope, \
    DBContentFilterType, DBContentFilterActionType
from waii_sdk_py.history import GetHistoryRequest, GeneratedHistoryEntryType
from waii_sdk_py.query import QueryGenerationRequest

from tests.log_util import init_logger
from tests.utils import wait_for_connector_status, verify_sample_values, like_query

"""
- Sample test case to add a Postgres connection
    - Setup will add the connection with content filters
    - Actual test will do the following
        1. Verify if sample values are available
        2. Generate a query
        3. Like the query
        3. Verify if liked query is present in history
"""

CONN_KEY = "postgresql://waii@localhost:5432/test"

CONNECTION = {
    "key": "postgresql://waii@localhost:5432/test",
    "db_type": "postgresql",
    "password": "password",
    "description": None,
    "username": "waii",
    "database": "test",
    "host": "localhost",
    "port": "5432",
    "sample_col_values": True,
    "push": False,
    "embedding_model": "text-embedding-ada-002",
    "db_access_policy": {
        "read_only": False,
        "allow_access_beyond_db_content_filter": True,
        "allow_access_beyond_search_context": True
    }
}

# Init the logger for this class
logger = init_logger(log_file="logs/test_basic_postgres_add.log")


@pytest.mark.docker_config("waii_default")
class Test_Basic_Postgres_Add:

    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"
        cls.apiclient = api_client
        cls.add_db_connection(api_client)

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.apiclient = api_client

    @staticmethod
    def add_db_connection(client):
        db_conn = DBConnection(**CONNECTION)
        db_conn.db_content_filters = [DBContentFilter(
            filter_scope=DBContentFilterScope.table,
            filter_type=DBContentFilterType.include,
            filter_action_type=DBContentFilterActionType.visibility,
            pattern='(DB_OWNER_STORAGE|USERS|PARAMETERS)'
        )]

        response = client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        logger.info(f"Check Connector status: {CONN_KEY}")
        connector_statuses = client.database.get_connections().connector_status
        logger.info(f"Local Connector status: {connector_statuses}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(client, CONN_KEY, retry=60, logger=logger)
        assert status is True, f"Connection for alias {CONN_KEY} is not ready."

    def test_add_postgres_connection(self, docker_environment):
        """
        1. Verify if sample values are available
        2. Generate a query
        3. Like the query
        3. Verify if liked query is present in history
        """

        client = self.apiclient
        logger.info(f"Running test_add_postgres_connection; resource = {self.resource}")
        try:

            # Verify if sample value is available
            verify_sample_values(client, "DB_OWNER_STORAGE", "DB_KEY", should_be_none=False)


            # Run a query and verify it generates query
            ask = "show me the total parameters available in tweakit schema"
            response = client.query.generate(params=QueryGenerationRequest(ask=ask))
            assert response is not None and response.query is not None, "Response should not be None"
            logger.info(f"Query response: {response.query}")

            # Verify if the query can be liked without issues
            like_query(ask, response.query, client)

            # Check if the ask is available in history
            history = client.history.get(params=GetHistoryRequest(included_types=[GeneratedHistoryEntryType.query], liked_query_filter=True))
            found = False
            for item in history.history:
                if item.request.ask == ask:
                    logger.info(f"Ask found in history (liked): {ask}")
                    found = True
                    break
            assert found, f"Ask '{ask}' not found in history (liked)"

        except Exception as e:
            logger.info(f"Error: {e}")
            assert False, f"Failed to add connection: {e}"
