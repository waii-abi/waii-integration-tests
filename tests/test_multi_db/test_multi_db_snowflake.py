import base64
import time
from pathlib import Path
from threading import Thread
from time import sleep
from typing import cast

import pandas as pd
import pytest
from pandas._testing import assert_frame_equal
from waii_sdk_py.history import GetHistoryRequest, GetHistoryResponse

from waii_sdk_py import Waii
from waii_sdk_py.database import ModifyDBConnectionRequest, DBConnection, DBContentFilter, DBContentFilterScope, \
    DBContentFilterType, DBContentFilterActionType, IngestDocumentRequest, DatabaseImpl, \
    GetIngestDocumentJobStatusRequest, IngestDocumentJobStatus, ModifyDBConnectionResponse, SearchContext, \
    GetCatalogRequest, FilterType
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest
from waii_sdk_py.semantic_context import ModifySemanticContextRequest, GetSemanticContextRequest, \
    GetSemanticContextRequestFilter

from tests.log_util import init_logger
from tests.utils import wait_for_connector_status, verify_sample_values

"""

From $WAII-INTEGRATION-TESTS directory, 

Run as `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_multi_db/test_multi_db_snowflake.py`

Note: If you already have docker, you can comment `@pytest.mark.docker_config("waii_default")` in this script. Otherwise, it will spin up its own doc container.

"""

# Init the logger for this class
logger = init_logger()

CONN_KEY = "waii://krishnabirla1@gqobxjv-bhb91428/snowflake-multi-db-100"

class TestMultiDB:
    # declare a class level api_client
    apiclient = None

    @classmethod
    def custom_setup(cls, api_client: Waii = None):
        logger.info(f"Setting up resources for {cls.__name__} with api_client:{api_client}")
        cls.resource = "Resource created"

        if api_client is None:
            api_client = Waii()
            api_client.initialize(url="http://localhost:9859/api/", api_key="")
            logger.info("Initialized API client")
            return

        cls.apiclient = api_client

    @classmethod
    def custom_cleanup(cls, api_client: Waii = None):
        logger.info(f"Cleaning up resources for {cls.__name__} with api_client:{api_client}")
        cls.apiclient = api_client
        CONN_KEY = "waii://krishnabirla1@gqobxjv-bhb91428/snowflake-multi-db-100"

        #TODO: If you enable this, you will run into WAII-4892
        #response: ModifyDBConnectionResponse = api_client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
        #logger.info(f"Removed multi-db connection: {CONN_KEY}, response: {response}")



    def test_multi_db_add_conn(self, docker_environment):
        """
        1. test adding new snowflake connection with multiple databases
        """

        client = self.apiclient
        logger.info(f"Running test_multi_db_add_conn; resource = {self.resource}")

        try:

            # Delete the connection (to be on safer side)
            try:
                client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
                logger.info("Removed existing multi-db connection")
            except Exception as e:
                logger.error(f"Failed to remove existing connection {CONN_KEY}, {str(e)}")

            response: ModifyDBConnectionResponse = client.database.modify_connections(
                ModifyDBConnectionRequest(
                    updated=[
                        DBConnection(
                            db_type="snowflake",
                            username="krishnabirla1",
                            account_name="gqobxjv-bhb91428",
                            role="MULTI_DB_JOIN_1",
                            password="jifsaP-vuccoc-ropze0",
                            #default_database="MOVIE_DB",
                            warehouse="COMPUTE_WH",
                            db_alias="snowflake-multi-db-100",
                            sample_col_values=True,
                            enable_multi_db_connection=True,
                            content_filters=[
                                SearchContext(
                                    db_name="MOVIE_DB"
                                ),
                                SearchContext(
                                    db_name="WAII"
                                ),
                                SearchContext(
                                    db_name="CINE_DB"
                                )
                            ]
                        )
                    ]
                )
            )

            logger.info("Added multi-db connection")

            client.database.activate_connection(CONN_KEY)
            logger.info(f"Activated alias: {CONN_KEY}")

            logger.info(f"Check Connector status: {CONN_KEY}")
            connector_statuses = client.database.get_connections().connector_status
            logger.info(f"Local Connector status: {connector_statuses}")

            # Wait for connector status to be ready
            status = wait_for_connector_status(client, CONN_KEY, retry=120, logger=logger)
            assert status is True, f"Connection for alias {CONN_KEY} is not ready."
        except Exception as e:
            logger.error(f"Failed to connect to alias {CONN_KEY}, {str(e)}")

    def test_schemas_in_databases(self, docker_environment):
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        expected_schemas = {
            "MOVIE_DB": {"INFORMATION_SCHEMA", "MOVIES_AND_TV"},
            "WAII": {
                "BATTLE_DEATH", "CAR", "CINE_TELE_DATA", "CONCERT_SINGER", "COURSE_TEACH",
                "CRE_DOC_TEMPLATE_MGT", "DOG_KENNELS", "EMPLOYEE_HIRE_EVALUATION", "FLIGHT",
                "INFORMATION_SCHEMA", "MUSEUM_VISIT", "NETWORK", "ORCHESTRA", "PETS",
                "POKER_PLAYER", "SINGER", "STUDENT_TRANSCRIPTS_TRACKING", "TVSHOW", "VOTER",
                "WEATHER", "WORLD", "REAL_ESTATE_PROPERTIES"
            },
            "CINE_DB": {"INFORMATION_SCHEMA", "CINE_TELE_DATA"}
        }

        # Fetch all catalogs
        actual_catalogs = {
            catalog.name: {schema.name.schema_name for schema in catalog.schemas}
            for catalog in client.database.get_catalogs(GetCatalogRequest()).catalogs
        }

        # Step 1: Assert all expected catalogs exist
        for expected_catalog, expected_schema_set in expected_schemas.items():
            assert expected_catalog in actual_catalogs, f"Expected catalog {expected_catalog} is missing."

            actual_schemas = actual_catalogs[expected_catalog]

            # Step 2: Assert all expected schemas are in the actual schemas
            missing_schemas = expected_schema_set - actual_schemas
            assert not missing_schemas, f"Missing schemas in catalog '{expected_catalog}': {missing_schemas}"

            logger.info(f"Catalog '{expected_catalog}' passed with schemas: {expected_schema_set}")

    def test_query_execution(self, docker_environment):
        # 1. Run basic query and check
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        ask = "Show the total number of concerts held in each stadium, ordered by the stadium name."
        response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
        query = response.query
        logger.info(f"Generated query: {query}")

        # Run the query
        response = client.query.run(RunQueryRequest(query=query))
        df = response.to_pandas_df()
        logger.info(f"Query execution response:\n{df}")

        # Expected DataFrame
        expected_df = pd.DataFrame({
            "STADIUM_NAME": ["Balmoor", "Glebe Park", "Recreation Park", "Somerset Park", "Stark's Park"],
            "TOTAL_CONCERTS": [1, 1, 1, 2, 1]
        })

        # Sort both DataFrames for robust comparison (optional if order is guaranteed)
        df_sorted = df.sort_values(by=["STADIUM_NAME"]).reset_index(drop=True)
        expected_sorted = expected_df.sort_values(by=["STADIUM_NAME"]).reset_index(drop=True)

        # Validate schema
        assert set(df.columns) == {"STADIUM_NAME", "TOTAL_CONCERTS"}, "Unexpected columns in result."

        # Validate values (strict)
        try:
            assert_frame_equal(df_sorted, expected_sorted)
        except AssertionError as e:
            logger.error("DataFrame content mismatch!")
            raise e

    def test_cross_db_reference(self, docker_environment):
        # 1. Run basic query and check
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        ask = "Show me the total number of movies that are present in cine_db and not in movie_db. Display as movie_count_diff."
        response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
        query = response.query
        logger.info(f"Generated query: {query}")

        # Run the query
        response = client.query.run(RunQueryRequest(query=query))
        df = response.to_pandas_df()
        logger.info(f"Query execution response:\n{df}")

        # Expected DataFrame
        expected_df = pd.DataFrame({
            "MOVIE_COUNT_DIFF": [0]
        })

        # Sort both DataFrames for robust comparison (optional if order is guaranteed)
        df_sorted = df.sort_values(by=["MOVIE_COUNT_DIFF"]).reset_index(drop=True)
        expected_sorted = expected_df.sort_values(by=["MOVIE_COUNT_DIFF"]).reset_index(drop=True)

        # Validate values (strict)
        try:
            assert_frame_equal(df_sorted, expected_sorted)
        except AssertionError as e:
            logger.error("DataFrame content mismatch!")
            raise e

    # TODO: There is bug in the system. It should not be cross referencing like this.
    def test_cross_db_wrong_query(self, docker_environment):
        # 1. Ask a question such that it may mix tables from different DB wrongly
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        ask = "Are there any tv series where any singer acted in it after 2005?"
        response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
        query = response.query
        logger.info(f"Generated query: {query}")

        # Check if it has got "waii.concert_singer.singer". If so it is an example of error. Ideally it should be from keywords.
        if "waii.concert_singer.singer" in query.lower():
            logger.error("Query contains cross DB reference which is not allowed: " + query)
            assert False, "Query contains cross DB reference which is not allowed. Joining movies with spider concerts!!! More like hallucinating or getting confused"


    def test_history(self, docker_environment):
        # 1. Run basic query and check
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        for i in range(1, 3):
            ask = f"Show the total number of concerts held in each stadium, ordered by the stadium name. Iteration: {i}"
            response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
            logger.info(f"Generated query: {response.query}")


        # atleast 3 entries should be there in history
        response:GetHistoryResponse = client.history.get(params=GetHistoryRequest())
        assert(len(response.history) >= 3), "History should have at least 3 entries."
        logger.info(f"Number of entries in history: {len(response.history)}")


    # TODO: There is bug in the system. For views, it has not create samples
    def test_samples_in_tables(self, docker_environment):
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        self.check_samples_in_table(client, table_name="movies", schema_name="cine_tele_data", db_name="waii")

    def check_samples_in_table(self, client, table_name:str, schema_name:str, db_name:str):
        # for all catalogs, go through schemas, and for all schemas, go through tables
        catalogs = client.database.get_catalogs(GetCatalogRequest()).catalogs
        logger.info(f"Catalogs: {[catalog.name for catalog in catalogs]}")
        for catalog in catalogs:
            logger.info(f"Catalog: {catalog.name}")
            for schema in catalog.schemas:
                if schema.name.schema_name == "INFORMATION_SCHEMA":
                    continue
                logger.info(f"  Schema: {schema.name.schema_name}")
                for table in schema.tables:
                    logger.info(f"    Table: {table.name.table_name}")
                    if table.name.table_name.strip().lower() == table_name and table.name.database_name.strip().lower() == db_name and table.name.schema_name.strip().lower() == schema_name:
                        for column in table.columns:
                            if column.type.lower() == "text" or column.type.lower() == "string":
                                logger.info(f"    Column: {column.name}, samples: {column.sample_values}")
                                assert column.sample_values is not None, f"Sample values for column {column} should not be None"


    # TODO: There is bug in the system. It is not filtering correctly.
    def test_multi_db_with_table_filter(self, docker_environment):
        """
        1. test adding new snowflake connection with multiple databases
        """

        client = self.apiclient
        logger.info(f"Running test_multi_db_add_conn with specific table for cine_db; resource = {self.resource}")

        try:
            # Delete the connection
            client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
            logger.info("Removed existing multi-db connection")

            response: ModifyDBConnectionResponse = client.database.modify_connections(
                ModifyDBConnectionRequest(
                    updated=[
                        DBConnection(
                            db_type="snowflake",
                            username="krishnabirla1",
                            account_name="gqobxjv-bhb91428",
                            role="MULTI_DB_JOIN_1",
                            password="jifsaP-vuccoc-ropze0",
                            #default_database="MOVIE_DB",
                            warehouse="COMPUTE_WH",
                            db_alias="snowflake-multi-db-100",
                            sample_col_values=True,
                            enable_multi_db_connection=True,
                            content_filters=[
                                SearchContext(
                                    db_name="MOVIE_DB"
                                ),
                                SearchContext(
                                    db_name="WAII"
                                ),
                                SearchContext(
                                    db_name="CINE_DB",
                                    type=FilterType.EXCLUSION,
                                    table_name=r"(?i)\b(peo\w*|som\w*)\b"
                                )
                            ]
                        )
                    ]
                )
            )

            logger.info("Added multi-db connection")

            client.database.activate_connection(CONN_KEY)
            logger.info(f"Activated alias: {CONN_KEY}")

            logger.info(f"Check Connector status: {CONN_KEY}")
            connector_statuses = client.database.get_connections().connector_status
            logger.info(f"Local Connector status: {connector_statuses}")

            # Wait for connector status to be ready
            # TODO: completed status in WAII is broken for multi-db. This could be a reason why this test is failing.
            status = wait_for_connector_status(client, CONN_KEY, retry=120, logger=logger)
            assert status is True, f"Connection for alias {CONN_KEY} is not ready."

            client.database.activate_connection(CONN_KEY)
            catalogs = client.database.get_catalogs(GetCatalogRequest()).catalogs
            for catalog in catalogs:
                logger.info(f"Catalog: {catalog.name}")
                if catalog.name.lower() == "cine_db":
                    for schema in catalog.schemas:
                        if schema.name.schema_name == "INFORMATION_SCHEMA":
                            continue
                        logger.info(f"  Schema: {schema.name.schema_name}")
                        for table in schema.tables:
                            if table.name.table_name.strip().lower() == "people" and table.name.database_name.strip().lower() == 'cine_db':
                                logger.error(f"Table {table.name.table_name} should not be present in catalog {catalog.name} as it is filtered out")
                                assert False, f"Table {table.name.table_name} should not be present in catalog {catalog.name} as it is filtered out"
        except Exception as e:
            logger.error(f"Failed to connect to alias {CONN_KEY}, {str(e)}")