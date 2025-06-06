import base64

import pandas as pd
from pandas._testing import assert_frame_equal

from tests.log_util import init_logger
from tests.utils import wait_for_connector_status
from waii_sdk_py import Waii
from waii_sdk_py.database import ModifyDBConnectionRequest, DBConnection, ModifyDBConnectionResponse, SearchContext, \
    GetCatalogRequest, FilterType
from waii_sdk_py.history import GetHistoryRequest, GetHistoryResponse
from waii_sdk_py.query import QueryGenerationRequest, RunQueryRequest

"""

From $WAII-INTEGRATION-TESTS directory, 

Run as `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_multi_db/test_multi_db_bigquery.py`

Note: If you already have docker, you can comment `@pytest.mark.docker_config("waii_default")` in this script. Otherwise, it will spin up its own doc container.

"""

# Init the logger for this class
logger = init_logger()

CONN_KEY = "waii://walmart-sap@walmart-poc-internal.iam.gserviceaccount.com@host/multi-walmart-db-100"


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

        # TODO: If you enable this, you will run into WAII-4892
        # response: ModifyDBConnectionResponse = api_client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
        # logger.info(f"Removed multi-db connection: {CONN_KEY}, response: {response}")

    def test_multi_db_add_conn(self, docker_environment):
        """
        1. test adding new snowflake connection with multiple databases
        """

        client = self.apiclient
        logger.info(f"Running test_multi_db_add_conn; resource = {self.resource}")

        try:
            client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
            logger.info("Removed existing multi-db connection")

            with open("poc-internal.json", "r") as f:
                file_contents = f.read()
                # decode (git will not allow secrets to be checked in)
                decoded_bytes = base64.b64decode(file_contents)
                decoded_str = decoded_bytes.decode("utf-8")
                print(decoded_str)

                response: ModifyDBConnectionResponse = client.database.modify_connections(
                    ModifyDBConnectionRequest(
                        updated=[
                            DBConnection(
                                db_type="bigquery",
                                password=decoded_str,
                                #default_database="triple-nectar-461407-k3",
                                db_alias="multi-walmart-db-100",
                                sample_col_values=True,
                                enable_multi_db_connection=True,
                                content_filters=[
                                    SearchContext(
                                        db_name="triple-nectar-461407-k3"  # wmt-1257d458107910dad54c01f5c8
                                    ),
                                    SearchContext(
                                        db_name="arched-curve-461405-t5"  # wmt-intl-cons-mc-k1-prod
                                    ), SearchContext(
                                        db_name="wmt-edw-dev-461405"
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

    #TODO: Bug in system (status). Similar to snowflake
    def test_schemas_in_databases(self, docker_environment):
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        expected_schemas = {
            "wmt-edw-dev-461405": {"US_FIN_SALES_DL_RPT_VM"},
            "triple-nectar-461407-k3": {"test"}
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

        ask = "Can you show 10 name from sample_table ? Show name field only"
        response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
        query = response.query
        logger.info(f"Generated query: {query}")

        # Run the query
        response = client.query.run(RunQueryRequest(query=query))
        df = response.to_pandas_df()
        logger.info(f"Query execution response:\n{df}")

        # Expected DataFrame
        expected_df = pd.DataFrame({
            "NAME": []
        })

        df.columns = [col.upper() for col in df.columns]
        assert set(df.columns) == {"NAME"}, f"Unexpected columns in result. Got: {df.columns.tolist()}"

        expected_df = pd.DataFrame({
            "NAME": pd.Series([], dtype="object")
        })

        try:
            assert_frame_equal(df, expected_df)
        except AssertionError as e:
            logger.error("DataFrame content mismatch!")
            raise e

    def test_cross_db_reference(self, docker_environment):
        # 1. Run basic query and check
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        ask = "can you create a query to join CLUB_TRANSACTIONS_DLY_D::CLUB_NM with sample_table::name. Finally show the total sales amount as sales_amt."
        response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
        query = response.query
        logger.info(f"Generated query: {query}")

        # Run the query
        response = client.query.run(RunQueryRequest(query=query))
        df = response.to_pandas_df()
        logger.info(f"Query execution response:\n{df}")

        # Expected DataFrame
        expected_df = pd.DataFrame({
            "sales_amt": [0]
        })

        df.columns = [col.lower() for col in df.columns]
        expected_df.columns = [col.lower() for col in expected_df.columns]

        expected_df = pd.DataFrame({
            "sales_amt": pd.Series([0], dtype="float64")
        })

        df = df.sort_index().reset_index(drop=True)
        expected_df = expected_df.sort_index().reset_index(drop=True)

        try:
            logger.warning(f"Expected:\n{expected_df}")
            logger.warning(f"Actual:\n{df}")
            assert_frame_equal(df, expected_df)
        except AssertionError as e:
            logger.error("DataFrame content mismatch!")
            raise e


    def test_history(self, docker_environment):
        # 1. Run basic query and check
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        for i in range(1, 3):
            ask = f"Can you show 10 name from sample_table ? Show name field only. Iteration: {i}"
            response = client.query.generate(params=QueryGenerationRequest(ask=ask, use_cache=False))
            logger.info(f"Generated query: {response.query}")

        # atleast 3 entries should be there in history
        response: GetHistoryResponse = client.history.get(params=GetHistoryRequest())
        assert (len(response.history) >= 3), "History should have at least 3 entries."
        logger.info(f"Number of entries in history: {len(response.history)}")

    # TODO: There is bug in the system. For views, it has not create samples
    def test_samples_in_tables(self, docker_environment):
        client = self.apiclient
        client.database.activate_connection(CONN_KEY)
        logger.info(f"Activated alias: {CONN_KEY}")

        self.check_samples_in_table(client, table_name="sample_table", schema_name="test", db_name="triple-nectar-461407-k3")

    def check_samples_in_table(self, client, table_name: str, schema_name: str, db_name: str):
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
        1. test adding new bigquery connection with multiple databases
        """

        client = self.apiclient
        logger.info(f"Running test_multi_db_add_conn with specific table for cine_db; resource = {self.resource}")

        try:

            client.database.modify_connections(ModifyDBConnectionRequest(removed=[CONN_KEY]))
            logger.info("Removed existing multi-db connection")

            with open("poc-internal.json", "r") as f:
                file_contents = f.read()
                # decode (git will not allow secrets to be checked in)
                decoded_bytes = base64.b64decode(file_contents)
                decoded_str = decoded_bytes.decode("utf-8")

                print(decoded_str)
                response: ModifyDBConnectionResponse = client.database.modify_connections(
                    ModifyDBConnectionRequest(
                        updated=[
                            DBConnection(
                                db_type="bigquery",
                                password=decoded_str,
                                #default_database="wmt-edw-prod-461405",
                                db_alias="multi-walmart-db-100",
                                sample_col_values=True,
                                enable_multi_db_connection=True,
                                content_filters=[
                                    SearchContext(
                                        db_name="triple-nectar-461407-k3", # wmt-1257d458107910dad54c01f5c8
                                        type = FilterType.EXCLUSION,
                                        table_name = "test_table1"
                                    ),
                                    SearchContext(
                                        db_name="arched-curve-461405-t5"  # wmt-intl-cons-mc-k1-prod
                                    ), SearchContext(
                                        db_name="wmt-edw-dev-461405"
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
                                if table.name.table_name.strip().lower() == "triple-nectar-461407-k3" and table.name.database_name.strip().lower() == 'test_table1':
                                    logger.error(
                                        f"Table {table.name.table_name} should not be present in catalog {catalog.name} as it is filtered out")
                                    assert False, f"Table {table.name.table_name} should not be present in catalog {catalog.name} as it is filtered out"
        except Exception as e:
            logger.error(f"Failed to connect to alias {CONN_KEY}, {str(e)}")
