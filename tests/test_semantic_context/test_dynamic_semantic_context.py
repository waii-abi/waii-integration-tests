import pytest
from waii_sdk_py import Waii
from waii_sdk_py.database import SearchContext
from waii_sdk_py.semantic_context import SemanticStatement
from waii_sdk_py.query import QueryGenerationRequest

from tests.log_util import init_logger
from tests.utils import add_db_connection

"""

From $WAII-INTEGRATION-TESTS directory, 

Run as `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_semantic_context/test_dynamic_semantic_context.py`

Note: If you already have docker, you can comment `@pytest.mark.docker_config("waii_default")` in this script. Otherwise, it will spin up its own doc container.

- Test confidence score
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

DB_NAME = "test"
SCHEMA_NAME = "TWEAKIT"
# Init the logger for this class
logger = init_logger(log_file="logs/test_dynamic_semantic_context.log")

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

    def _get_rag_context(self, include_scope=False):
        return SemanticStatement(
            statement="**If the user asks questions involving 'I' or 'me' or 'current user', they are referring to the user_name 'integration_test' ",
            scope="test.waii.users" if include_scope else "*",
            always_include=False,
            lookup_summaries=["I", "me", "current user"],
        )

    def _get_always_include_context(self, include_scope=False):
        return SemanticStatement(
            statement="**If the user asks questions involving 'I' or 'me' or 'current user', they are referring to the user_name 'integration_test' ",
            scope="test.waii.users" if include_scope else "*",
            always_include=True,
        )

    def _gen_query_and_check(self, context, use_chat=False):
        question = "How many db connections does the current user have?"
        if use_chat:
            from waii_sdk_py.chat.chat import ChatRequest
            request = ChatRequest(
                ask=question,
                additional_context=[context],
                search_context=[SearchContext(db_name='TEST', schema_name='WAII')],
            )
            response = self.apiclient.chat.chat_message(request)
            logger.info(f"Chat message response: {response}")
            assert response.response_data is not None, "No response_data in chat response"
            assert response.response_data.query is not None, "No query in chat response_data"
            assert 'integration_test' in response.response_data.query.query, "Expected 'integration_test' in chat query"
            return response
        else:
            request = QueryGenerationRequest(
                ask=question,
                additional_context=[context],
                search_context=[SearchContext(db_name='TEST', schema_name='WAII')],
            )
            result = self.apiclient.query.generate(request)
            logger.info(f"Generated query: {result.query}")
            assert result.query is not None
            assert 'integration_test' in result.query
            return result

    # Chat-based tests
    def test_chat_rag_context(self, docker_environment):
        context = self._get_rag_context()
        self._gen_query_and_check(context, use_chat=True)

    def test_chat_rag_context_with_scope(self, docker_environment):
        context = self._get_rag_context(include_scope=True)
        self._gen_query_and_check(context, use_chat=True)

    def test_chat_always_include_context(self, docker_environment):
        context = self._get_always_include_context()
        self._gen_query_and_check(context, use_chat=True)

    def test_chat_always_include_context_with_scope(self, docker_environment):
        context = self._get_always_include_context(include_scope=True)
        self._gen_query_and_check(context, use_chat=True)

    # Query-based tests
    def test_rag_context(self, docker_environment):
        context = self._get_rag_context()
        self._gen_query_and_check(context)

    def test_rag_context_with_scope(self, docker_environment):
        context = self._get_rag_context(include_scope=True)
        self._gen_query_and_check(context)

    def test_always_include_context(self, docker_environment):
        context = self._get_always_include_context()
        self._gen_query_and_check(context)

    def test_always_include_context_with_scope(self, docker_environment):
        context = self._get_always_include_context(include_scope=True)
        self._gen_query_and_check(context)

    @classmethod
    def custom_cleanup(cls, api_client):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.apiclient = api_client