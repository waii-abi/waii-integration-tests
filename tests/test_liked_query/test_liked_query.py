import time
import pandas as pd
from uuid import uuid4

import pytest
from waii_sdk_py.database import DBConnection, ModifyDBConnectionRequest, DBConnectionIndexingStatus
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

from tests.utils import wait_for_connector_status, like_query

CONN_KEY = "snowflake://WAII@gqobxjv-bhb91428/MOVIE_DB?role=WAII_USER_ROLE&warehouse=COMPUTE_WH"

CONNECTION = {
    "key": "waii://WAII@gqobxjv-bhb91428/movie_integ_liked_query_1",
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
    "db_alias": 'movie_integ_liked_query_1',
}


def test_liked_query(api_client):
    try:
        api_client.database.activate_connection(CONN_KEY)
        print(f"Activated connection: {CONN_KEY}")

        # Create connection (alias)
        ALIAS_KEY = "waii://WAII@gqobxjv-bhb91428/movie_integ_liked_query_1"
        db_conn = {**CONNECTION}
        db_conn["db_alias"] = "movie_integ_liked_query_1"
        db_conn["key"] = ALIAS_KEY
        response = api_client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))
        api_client.database.activate_connection(ALIAS_KEY)
        print(f"Activated alias: {ALIAS_KEY}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(api_client, ALIAS_KEY)
        assert status is True, f"Connection for alias {ALIAS_KEY} is not ready."

        # like a query
        question = "Identify the top 10 TV series with the highest number of votes, and include their origin language."
        query = """
        WITH ranked_series AS (
            SELECT
                asset_title,
                origin_language,
                vote_count,
                ROW_NUMBER() OVER (ORDER BY vote_count DESC NULLS LAST) AS rank
            FROM movie_db.movies_and_tv.tv_series
        )
        
        SELECT
            asset_title,
            origin_language,
            vote_count
        FROM ranked_series
        WHERE
            rank <= 10
        """
        like_query(question, query, api_client)
        time.sleep(5)

        # Ask a question that is close to "liked" query. It should pick up from liked query.
        question = "Show me the TV series with the highest number of votes, and include their origin language. Show me top 10 records."
        uuid = str(uuid4())
        response = api_client.query.generate(params=QueryGenerationRequest(ask=question, uuid=uuid))
        response = api_client.query.run(params=RunQueryRequest(query=response.query))
        generated_df = response.to_pandas_df()

        # Execute the query for the liked query
        liked_query_response = api_client.query.run(params=RunQueryRequest(query=query))
        liked_query_generated_df = liked_query_response.to_pandas_df()

        # Compare the two DataFrames
        try:
            pd.testing.assert_frame_equal(liked_query_generated_df, generated_df, check_dtype=False)
            print("The result sets match.")
        except AssertionError as e:
            pytest.fail(f"Result sets do not match: {e}")
    except Exception as e:
        pytest.fail(f"Error checking liked queries: {e}")

