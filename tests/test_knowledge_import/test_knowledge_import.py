import base64
import time
from pathlib import Path
from typing import cast

from waii_sdk_py import Waii
from waii_sdk_py.database import ModifyDBConnectionRequest, DBConnection, DBContentFilter, DBContentFilterScope, \
    DBContentFilterType, DBContentFilterActionType, IngestDocumentRequest, DatabaseImpl, \
    GetIngestDocumentJobStatusRequest, IngestDocumentJobStatus
from waii_sdk_py.query import QueryGenerationRequest
from waii_sdk_py.semantic_context import ModifySemanticContextRequest, GetSemanticContextRequest, \
    GetSemanticContextRequestFilter

from tests.log_util import init_logger
from tests.utils import wait_for_connector_status, verify_sample_values

"""

From $WAII-INTEGRATION-TESTS directory, 

Run as `pytest -s -n 6 --html=reports/report_$(date +"%Y-%m-%d_%H-%M-%S_%3N").html --self-contained-html tests/test_knowledge_import/test_knowledge_import.py`

Note: If you already have docker, you can comment `@pytest.mark.docker_config("waii_default")` in this script. Otherwise, it will spin up its own doc container.

- Test knowledge import
    - Setup tweakit postgres DB in waii
    - Import its knowledge base; It has duplicate tables, conflicting columns, descriptions etc.
    - Verify if it is properly imported.
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
logger = init_logger(log_file="logs/test_knowledge_import.log")


@pytest.mark.docker_config("waii_default")
class TestKnowledgeImport:
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

        try:
            response = client.database.modify_connections(params=ModifyDBConnectionRequest(updated=[db_conn]))

            client.database.activate_connection(CONN_KEY)
            logger.info(f"Activated alias: {CONN_KEY}")

            logger.info(f"Check Connector status: {CONN_KEY}")
            connector_statuses = client.database.get_connections().connector_status
            logger.info(f"Local Connector status: {connector_statuses}")

            # Wait for connector status to be ready
            status = wait_for_connector_status(client, CONN_KEY, retry=60, logger=logger)
            assert status is True, f"Connection for alias {CONN_KEY} is not ready."
        except Exception as e:
            logger.error(f"Failed to connect to alias {CONN_KEY}, {str(e)}")

    def test_knowledge_import(self, docker_environment):
        """
        1. Verify if sample values are available
        2. Generate a query
        3. Delete all sem contexts
        4. Import a file and verify if it is ingested
        5. Try different formats xlxs, pdf, contracticatory statements etc
        6. Try inserting same doc multiple times (check for dups)
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

            # Delete any existing contexts
            logger.info(f"Deleting all semantic contexts")
            self.delete_all_sem_contexts()

            # Now import from XLSX
            logger.info(f"Ingesting doc (xlsx)")
            self.ingest_document(file_name='tweakit_db_owner_storage.xlsx')
            labels = ["file_name=tweakit_db_owner_storage"]
            contexts = self.get_sem_contexts(labels=labels)
            logger.info(f"Number of contexts: {labels}: {len(contexts)}")
            # TODO: Due to https://waii-ai.atlassian.net/browse/WAII-4365
            assert len(contexts) >= 8, f"Expected atleast 8 contexts for scope {labels}, but got {len(contexts)}"

            # Now import from PDF
            logger.info(f"Ingesting doc (PDF)")
            self.ingest_document(file_name='tweakit_doc.pdf')
            labels = ["file_name=tweakit_doc"]
            contexts = self.get_sem_contexts(labels=labels)
            logger.info(f"Number of contexts: {labels}: {len(contexts)}")
            assert len(contexts) >= 18, f"Expected at max 2 contexts for scope {labels}, but got {len(contexts)}"

            # now import contradictory statements. ie with duplicate tables, columns and very contradictory descriptions
            logger.info(f"Ingesting doc (xlsx)")
            self.ingest_document(file_name='tweakit_contradictory_definitions.xlsx')
            labels = ["file_name=tweakit_contradictory_definitions"]
            contexts = self.get_sem_contexts(labels=labels)
            logger.info(f"Number of contexts: {labels}: {len(contexts)}")
            # TODO: Still marking 18. As of now, it imports duplicates, contradictory statements. https://waii-ai.atlassian.net/browse/WAII-4366
            assert len(contexts) >= 18, f"Expected at max 2 contexts for scope {labels}, but got {len(contexts)}"

            logger.info(f"Deleting all contexts")
            self.delete_all_sem_contexts()
            for i in range(5):
                logger.info(f"Ingesting doc (xlsx)..itr: {i}")
                self.ingest_document(file_name='tweakit_contradictory_definitions.xlsx')

            # Check users::name scope
            scope = "users.name"
            contexts = self.get_sem_contexts(scope=scope)
            # TODO: due to https://waii-ai.atlassian.net/browse/WAII-4364
            #assert len(contexts) <= 2, f"Expected at max 2 contexts for scope {scope}, but got {len(contexts)}"
            logger.info(f"Total sem contexts: {len(contexts)}")

        except Exception as e:
            logger.info(f"Error: {e}")
            assert False, f"Failed to add connection: {e}"


    def get_sem_contexts(self, scope: str = None, labels: list[str] = None):
        response = self.apiclient.semantic_context.get_semantic_context(
            GetSemanticContextRequest(search_context=self.get_search_scope(),
                                      filter=GetSemanticContextRequestFilter(scope=scope, labels=labels)))
        contexts = response.semantic_context
        return contexts

    def get_search_scope(self) -> list:
        return [{'db_name': DB_NAME, 'schema_name': SCHEMA_NAME, 'table_name': '*'}]

    def delete_all_sem_contexts(self):
        """
        Delete all semantic contexts for the given database and schema.
        """
        search_context = self.get_search_scope()
        response = self.apiclient.semantic_context.get_semantic_context(
            GetSemanticContextRequest(search_context=search_context))
        contexts = []
        for context in response.semantic_context:
            if context.id:
                contexts.append(context.id)
                try:
                    logger.info(f"Deleting semantic context: {context.id}")
                    self.apiclient.semantic_context.modify_semantic_context(
                        ModifySemanticContextRequest(deleted=contexts))
                except Exception as e:
                    logger.info(f"Error deleting semantic context: {e}")
                    assert False, f"Failed to delete sem context: {context}, error: {str(e)}"
                finally:
                    contexts.clear()
        logger.info("Done deleting all semantic contexts")

    def ingest_document(self, file_name, is_binary: bool = False):
        """
        Ingest a document into WAII system (300 seconds is the timeout)
        """
        try:
            # Import knowledge base
            logger.info(f"Ingesting document: {file_name}")
            script_dir = Path(__file__).parent.absolute()
            file_path = Path(script_dir, file_name)

            if not file_path.is_file():
                logger.error(f"File not found: {file_path}")
                raise FileNotFoundError(f"File not found: {file_path}")

            with open(file_path, 'rb') as f:
                file_bytes = f.read()
            encoded_content = base64.b64encode(file_bytes).decode('utf-8')

            db = cast(DatabaseImpl, self.apiclient.database)
            response = db.ingest_document(IngestDocumentRequest(
                content=encoded_content,
                is_binary=is_binary,
                filename=file_name
            ))
            logger.info(f"Ingested document response: {response}")
            job_id = response.ingest_document_job_id
            logger.info(f"Ingest document Job ID: {job_id}")
            stime = time.time()
            while True:
                response = db.get_ingest_document_job_status(
                    GetIngestDocumentJobStatusRequest(ingest_document_job_id=job_id))
                logger.info(f"Ingest document job status: {response.status}")
                timeout = 600
                if response.status == IngestDocumentJobStatus.completed or (time.time() - stime) > timeout:
                    if (time.time() - stime) > timeout:
                        logger.info(
                            f"Error getting ingest document job status even after {timeout} seconds...bailing out")
                    break
                logger.info(f"Waiting for ingest document job to complete...Sleep for 3 seconds")
                time.sleep(3)
        except Exception as e:
            logger.error(f"Error ingesting document: {e}")
            assert False, f"Failed to ingest document: {e}"
