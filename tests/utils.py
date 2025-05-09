import hashlib
import logging
import time

import pytest
from waii_sdk_py import Waii
from waii_sdk_py.database import DBConnectionIndexingStatus, DBConnection, ModifyDBConnectionRequest
from waii_sdk_py.query import LikeQueryRequest


def wait_for_connector_status(api_client, alias_key, retry=10, logger:logging.Logger = None):
    # Get the connector status for the given alias_key
    connector_statuses = api_client.database.get_connections().connector_status
    if alias_key not in connector_statuses:
        pytest.fail(f"No connector status found for key: {alias_key}")

    db_conn_status: DBConnectionIndexingStatus = connector_statuses[alias_key]

    # Retry for a maximum of 60 seconds (retry every 10 seconds, up to 6 times)
    status = db_conn_status.status
    while retry > 0:
        if status is None or status != 'completed':
            logger.info(f"About to get connected status for : {alias_key}; Will retry {retry} times, {connector_statuses[alias_key]}")
            time.sleep(10)
            retry -= 1
            status = api_client.database.get_connections().connector_status[alias_key].status
        else:
            break

    if status == 'completed':
        logger.info(f"Connection ready for {alias_key}: status: {db_conn_status.status}")
        return True
    else:
        return False

def add_db_connection(client, connection, conn_key, logger):
    db_conn = DBConnection(**connection)
    # db_conn.db_content_filters = [DBContentFilter(
    #     filter_scope=DBContentFilterScope.table,
    #     filter_type=DBContentFilterType.include,
    #     filter_action_type=DBContentFilterActionType.visibility,
    #     pattern='(DB_OWNER_STORAGE|USERS|PARAMETERS)'
    # )]

    try:
        response = client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

        client.database.activate_connection(conn_key)
        logger.info(f"Activated alias: {conn_key}")

        logger.info(f"Check Connector status: {conn_key}")
        connector_statuses = client.database.get_connections().connector_status
        logger.info(f"Local Connector status: {connector_statuses}")

        # Wait for connector status to be ready
        status = wait_for_connector_status(client, conn_key, retry=60, logger=logger)
        assert status is True, f"Connection for alias {conn_key} is not ready."
    except Exception as e:
        logger.error(f"Failed to connect to alias {conn_key}, {str(e)}")


def verify_sample_values(api_client, table_name, column_name, should_be_none):
    """
    Iterate through catalogs to verify the sample values for a given table column.
    """
    catalogs = api_client.database.get_catalogs().catalogs
    for catalog in catalogs:
        for schema in catalog.schemas:
            for table in schema.tables:
                if table.name.table_name.upper() == table_name.upper():
                    for col in table.columns:
                        if col.name.upper() == column_name.upper():
                            if should_be_none:
                                assert col.sample_values is None, (
                                    f"Sample values for {col.name} should be None"
                                )
                            else:
                                assert col.sample_values is not None, (
                                    f"Sample values for {col.name} should not be None"
                                )
                            return  # Found the column; exit the function.
    pytest.fail(f"Column {column_name} in table {table_name} not found.")


def like_query(question, query, client:Waii):
    question_hash = hashlib.md5(question.encode('utf-8')).hexdigest()
    client.query.like(LikeQueryRequest(query_uuid=question_hash, ask=question, query=query, liked=True))


def init_api_client(base_url, api_key):
    client = Waii()
    client.initialize(url=base_url, api_key=api_key)
    return client
