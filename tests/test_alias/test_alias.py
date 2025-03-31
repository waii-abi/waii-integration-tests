import time

import pytest
from waii_sdk_py.database import DBConnection, ModifyDBConnectionRequest, DBConnectionIndexingStatus, \
    UpdateSimilaritySearchIndexRequest, ColumnName, TableName
from waii_sdk_py.query import QueryGenerationRequest

from tests.utils import wait_for_connector_status, verify_sample_values

CONN_KEY = "snowflake://WAII@gqobxjv-bhb91428/MOVIE_DB?role=WAII_USER_ROLE&warehouse=COMPUTE_WH"

CONNECTION = {
    "key": "waii://WAII@gqobxjv-bhb91428/movie_integ_alias_2",
    "db_type": "snowflake",
    "password": "Waii_user_database_24!!",
    "description": None,
    "account_name": "gqobxjv-bhb91428",
    "username": "WAII",
    "database": "MOVIE_DB",
    "warehouse": "COMPUTE_WH",
    "role": "WAII_USER_ROLE",
    "path": None,
    "host": "",
    "port": None,
    "parameters": {},
    "sample_col_values": True,
    "push": False,
    "db_content_filters": None,
    "embedding_model": "text-embedding-ada-002",
    "always_include_tables": None,
    "alias": None,
    "db_access_policy": {
        "read_only": False,
        "allow_access_beyond_db_content_filter": True,
        "allow_access_beyond_search_context": True
    },
    "host_alias": None,
    "user_alias": None,
    "db_alias": 'movie_integ_alias_1',
}


def test_alias(api_client):
    """
    1. MOVIE_DB already exists in the docker
    2. Create an alias movie_integ_alias_1 for it.
    3. Activate it and verify the connection is ready.
    """
    try:
        ALIAS_KEY = "waii://WAII@gqobxjv-bhb91428/movie_integ_alias_1"
        api_client.database.activate_connection(CONN_KEY)
        print(f"Activated connection: {CONN_KEY}")

        db_conn = DBConnection(**CONNECTION)

        response = api_client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

        api_client.database.activate_connection(ALIAS_KEY)
        print(f"Activated alias: {ALIAS_KEY}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(api_client, ALIAS_KEY)
        assert status is True, f"Connection for alias {ALIAS_KEY} is not ready."

        # Get the sample values
        verify_sample_values(api_client, "EPISODIC_GROUPS", "EPISODE_NAME", should_be_none=False)
    except Exception as e:
        pytest.fail(f"Error creating alias: {e}")


def test_alias_without_sample_values(api_client):
    """
    1. MOVIE_DB already exists in the docker
    2. Create an alias movie_integ_alias_1_no_sample_values for it.
    3. Activate it and verify without sample values.
    """
    try:
        api_client.database.activate_connection(CONN_KEY)
        print(f"Activated connection: {CONN_KEY}")

        # Prepare alias without sample values
        ALIAS_KEY = "waii://WAII@gqobxjv-bhb91428/movie_integ_alias_1_no_sample_values"
        new_connection = {**CONNECTION}
        new_connection["sample_col_values"] = False
        new_connection["db_alias"] = "movie_integ_alias_1_no_sample_values"
        new_connection["key"] = ALIAS_KEY

        # Create connection (alias)
        db_conn = DBConnection(**new_connection)
        response = api_client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

        api_client.database.activate_connection(ALIAS_KEY)
        print(f"Activated alias: {ALIAS_KEY}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(api_client, ALIAS_KEY)
        assert status is True, f"Connection for alias {ALIAS_KEY} is not ready."

        # Get the sample values
        verify_sample_values(api_client, "EPISODIC_GROUPS", "EPISODE_NAME", should_be_none=True)

        # Run a query and verify results
        ask = "Show me the number of movies in the database"
        response = api_client.query.generate(params=QueryGenerationRequest(ask=ask))
        assert response is not None and response.query is not None, "Response should not be None"
        print(f"Query response: {response.query}")
    except Exception as e:
        pytest.fail(f"Error creating alias: {e}")


def test_alias_similarity_index(api_client):
    """
    1. MOVIE_DB already exists in the docker
    2. Create an alias movie_integ_alias_2_no_sample_values for it.
    3. Update similarity index for genre and check it.
    """
    try:
        api_client.database.activate_connection(CONN_KEY)
        print(f"Activated connection: {CONN_KEY}")

        # Prepare alias without sample values
        ALIAS_KEY = "waii://WAII@gqobxjv-bhb91428/movie_integ_alias_2_no_sample_values"
        new_connection = {**CONNECTION}
        new_connection["sample_col_values"] = False
        new_connection["db_alias"] = "movie_integ_alias_2_no_sample_values"
        new_connection["key"] = ALIAS_KEY

        # Create connection (alias)
        db_conn = DBConnection(**new_connection)
        response = api_client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

        api_client.database.activate_connection(ALIAS_KEY)
        print(f"Activated alias: {ALIAS_KEY}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(api_client, ALIAS_KEY)
        assert status is True, f"Connection for alias {ALIAS_KEY} is not ready."

        # Run a query and verify results
        ask = "What is the name of Politics genre?"
        response = api_client.query.generate(params=QueryGenerationRequest(ask=ask))

        # check if response.query contains "'%Politics%'"
        assert "'%Politics%'" in response.query, "Query does not contain Politics filter"

        # Get the sample values
        column = ColumnName(table_name=TableName(database_name="MOVIE_DB", schema_name="MOVIES_AND_TV", table_name="GENRE"), column_name="NAME")
        response = api_client.database.update_similarity_search_index(UpdateSimilaritySearchIndexRequest(column=column, values=[]))
        print(f"Updated similarity search index: {response}")

        # Run a query and verify results
        response = api_client.query.generate(params=QueryGenerationRequest(ask=ask))

        # check if response.query contains "'%Politics%'"
        assert "'War & Politics'" in response.query, "Query does not contain War & Politics filter"


    except Exception as e:
        pytest.fail(f"Error creating alias: {e}")


def test_query_gen_with_impersonation(api_client):
    """
    1. Impersonate as "sys_user@waii-service.ai"
    2. Generate a query in usage reporting DB
    """
    try:
        with api_client.impersonate_user(user_id="sys_user@waii-service.ai"):
            api_client.database.activate_connection("waii://waii@host/waii-usage-reporting")
            # Run a query and verify results
            ask = "show me the number of questions asked so far?"
            response = api_client.query.generate(params=QueryGenerationRequest(ask=ask))
            assert response is not None and response.query is not None, "Response should not be None"
            print(f"Query response: {response.query}")
    except Exception as e:
        pytest.fail(f"Error creating alias: {e}")
