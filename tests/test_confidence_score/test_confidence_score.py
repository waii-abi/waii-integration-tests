import base64
import time
from pathlib import Path
from typing import cast

import pytest
from waii_sdk_py import Waii
from waii_sdk_py.database import ModifyDBConnectionRequest, DBConnection, DBContentFilter, DBContentFilterScope, \
    DBContentFilterType, DBContentFilterActionType, IngestDocumentRequest, DatabaseImpl, \
    GetIngestDocumentJobStatusRequest, IngestDocumentJobStatus
from waii_sdk_py.query import QueryGenerationRequest, DebugInfoType
from waii_sdk_py.semantic_context import ModifySemanticContextRequest, GetSemanticContextRequest, \
    GetSemanticContextRequestFilter

from tests.log_util import init_logger
from tests.utils import wait_for_connector_status, verify_sample_values, add_db_connection

"""

From $WAII-INTEGRATION-TESTS directory, 

Run as `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_confidence_score/test_confidence_score.py`

Note: If you already have docker, you can comment `@pytest.mark.docker_config("waii_default")` in this script. Otherwise, it will spin up its own doc container.

- Test confidence score
"""

ORACLE_CONN_KEY = "oracle://movie_db_user@localhost:1521/movie_db"

ORACLE_CONNECTION = {
    "key": "oracle://movie_db_user@localhost:1521/movie_db",
    "db_type": "oracle",
    "password": "password",
    "description": None,
    "username": "movie_db_user",
    "database": "movie_db",
    "host": "localhost",
    "port": "1521",
    "sample_col_values": True,
    "push": False,
    "embedding_model": "text-embedding-ada-002",
    "db_access_policy": {
        "read_only": False,
        "allow_access_beyond_db_content_filter": True,
        "allow_access_beyond_search_context": True
    }
}

PG_CONN_KEY = "postgresql://waii@localhost:5432/test"

PG_CONNECTION = {
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

CONN_KEY = ORACLE_CONN_KEY
CONNECTION = ORACLE_CONNECTION

DB_NAME = "test"
SCHEMA_NAME = "TWEAKIT"
# Init the logger for this class
logger = init_logger(log_file="logs/test_confidence_score.log")


@pytest.mark.docker_config("krishna_birla_local")
class TestConfidenceScore:
    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"

        if api_client is None:
            api_client = Waii()
            api_client.initialize(url="http://localhost:9859/api/", api_key="")
            logger.info("Initialized API client")
            return

        cls.apiclient = api_client
        add_db_connection(api_client, CONNECTION, CONN_KEY, logger)

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.apiclient = api_client

    def test_confidence_score(self, docker_environment):
        """
        1. Verify if sample values are available
        2. Generate a query
        3. Delete all sem contexts
        4. Import a file and verify if it is ingested
        5. Try different formats xlxs, pdf, contracticatory statements etc
        6. Try inserting same doc multiple times (check for dups)
        """

        client = self.apiclient
        logger.info(f"Running test_confidence_score; resource = {self.resource}")
        try:

            # Verify if sample value is available
            # verify_sample_values(client, "DB_OWNER_STORAGE", "DB_KEY", should_be_none=False)

            # Run a query and verify it generates query
            # A liked query exists: "list any 5 movies"
            ask = "list any 5 films"
            response = client.query.generate(params=QueryGenerationRequest(ask=ask))
            assert response is not None and response.query is not None, "Response should not be None"
            logger.info(f"Query response: {response.query}, debug_info: {response.debug_info}")

            debug_info = response.debug_info
            assert debug_info is not None, "debug_info should not be None"

            required_keys = [
                "num_tables",
                "num_cols",
                "num_window_functions",
                "num_aggs",
                "num_set_ops",
                "semantic_context_available",
                "cosine_similarity_ask_query",
                "pk_join_count",
                "num_joins",
                "semantic_context_used",
                "retry_info",
                # One liked query should get matched at least
                # These only happen when queries are matched and FSL_SIMILARITY_SIGNAL_ENABLED is enabled
                "num_fsl_queries",
                "fsl_cosine_similarity"
            ]

            missing_keys = []

            # check if we have any missing keys in debug_info
            for key in required_keys:
                if key not in debug_info:
                    missing_keys.append(key)

            if len(missing_keys) > 0:
                logger.info(f"Missing keys in debug_info: {missing_keys}")
                assert False, f"Missing keys in debug_info: {missing_keys}"

        except Exception as e:
            logger.info(f"Error: {e}")
            assert False, f"Failed to add connection: {e}"


    def get_sem_contexts(self, scope: str = None, labels: list[str] = None):
        response = self.apiclient.semantic_context.get_semantic_context(
            GetSemanticContextRequest(search_context=self.get_search_scope(),
                                      filter=GetSemanticContextRequestFilter(scope=scope, labels=labels)))
        contexts = response.semantic_context
        return contexts
