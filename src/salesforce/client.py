import logging
from time import sleep
from urllib.parse import urlparse, urljoin

import backoff
import requests
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce.exceptions import SalesforceExpiredSession
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import SFType, Salesforce

from collections import OrderedDict
from .soql_query import SoqlQuery
from typing import List, Tuple, Iterator, Any, Dict


NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64"]

# Some objects are not supported by bulk and there is no exact way to determine them, they must be set like this
# https://help.salesforce.com/s/articleView?id=000383508&type=1
OBJECTS_NOT_SUPPORTED_BY_BULK = ["AccountFeed", "AssetFeed", "AccountHistory", "AcceptedEventRelation",
                                 "DeclinedEventRelation", "AggregateResult", "AttachedContentDocument", "CaseStatus",
                                 "CaseTeamMember", "CaseTeamRole", "CaseTeamTemplate", "CaseTeamTemplateMember",
                                 "CaseTeamTemplateRecord", "CombinedAttachment", "ContentFolderItem", "ContractStatus",
                                 "EventWhoRelation", "FolderedContentDocument", "KnowledgeArticleViewStat",
                                 "KnowledgeArticleVoteStat", "LookedUpFromActivity", "Name", "NoteAndAttachment",
                                 "OpenActivity", "OwnedContentDocument", "PartnerRole", "RecentlyViewed",
                                 "ServiceAppointmentStatus", "SolutionStatus", "TaskPriority", "TaskStatus",
                                 "TaskWhoRelation", "UserRecordAccess", "WorkOrderLineItemStatus", "WorkOrderStatus"]

DEFAULT_CHUNK_SIZE = 100000


class SalesforceClientException(Exception):
    pass


class QueryFailedException(Exception):
    pass


class SalesforceClient(SalesforceBulk):
    def __init__(self, simple_client: Salesforce, api_version: str, pk_chunking_size: int = DEFAULT_CHUNK_SIZE) -> None:
        # Initialize the client with from_connected_app or from_security_token, this creates a login with the
        # simple salesforce client. The simple_client sessionId is a Bearer token that is result of the login.
        super().__init__(sessionId=simple_client.session_id,
                         host=simple_client.sf_instance,
                         API_version=api_version)
        self.simple_client = simple_client
        self.pk_chunking_size = pk_chunking_size
        self.api_version = api_version
        self.host = urlparse(self.endpoint).hostname

    @classmethod
    def from_connected_app(cls, username: str, password: str, consumer_key: str, consumer_secret: str, sandbox: str,
                           api_version: str = DEFAULT_API_VERSION, pk_chunking_size: int = DEFAULT_CHUNK_SIZE,
                           domain: str = None):
        domain = 'test' if sandbox else domain

        simple_client = Salesforce(username=username, password=password, consumer_secret=consumer_secret,
                                   consumer_key=consumer_key,
                                   domain=domain, version=api_version)

        return cls(simple_client=simple_client, api_version=api_version, pk_chunking_size=pk_chunking_size)

    @classmethod
    def from_security_token(cls, username: str, password: str, security_token: str, sandbox: str, api_version: str,
                            pk_chunking_size: int = DEFAULT_CHUNK_SIZE,
                            domain: str = None):

        domain = 'test' if sandbox else domain
        simple_client = Salesforce(username=username, password=password, security_token=security_token,
                                   domain=domain, version=api_version)

        return cls(simple_client=simple_client, api_version=api_version, pk_chunking_size=pk_chunking_size)

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def describe_object(self, sf_object: str) -> List[str]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def describe_object_w_metadata(self, sf_object: str) -> List[Tuple[str, str]]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [(field['name'], field['type']) for field in object_desc['fields']
                if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def describe_object_w_complete_metadata(self, sf_object: str) -> Dict[str, Any]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return object_desc

    @staticmethod
    def is_bulk_supported_field(field: OrderedDict) -> bool:
        return field["type"] not in NON_SUPPORTED_BULK_FIELD_TYPES

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def run_query(self, soql_query: SoqlQuery) -> Iterator:
        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV', concurrency='Parallel')
        batch = self.query(job, soql_query.query)
        logging.info(f"Running SOQL : {soql_query.query}")

        try:
            while not self.is_batch_done(batch):
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        except ConnectionError as e:
            raise SalesforceClientException(f"Encountered error when running query: {e}") from e
        except SalesforceExpiredSession as e:
            raise SalesforceClientException(f"Encountered Expired Session error when running query: {e}") from e

        logging.info("SOQL ran successfully, fetching results")
        batch_result = self.get_all_results_from_query_batch(batch)
        return batch_result

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def test_query(self, soql_query: SoqlQuery) -> None:
        soql_query.add_limit()
        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV', concurrency='Parallel')
        batch = self.query(job, soql_query.query)
        logging.info(f"Running test SOQL : {soql_query.query}")

        try:
            while not self.is_batch_done(batch):
                sleep(10)
        except BulkBatchFailed as e:
            raise QueryFailedException(f"Test query failed: {e.state_message}") from e
        except ConnectionError as e:
            raise SalesforceClientException(f"Encountered error when running query: {e}") from e
        except SalesforceExpiredSession as e:
            raise SalesforceClientException(f"Encountered Expired Session error when running query: {e}") from e

        logging.info("Test query has been successful.")
        return

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def run_chunked_query(self, soql_query):
        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV', concurrency='Parallel',
                                       pk_chunking=self.pk_chunking_size)
        self.query(job, soql_query.query)
        logging.info(f"Running SOQL : {soql_query.query}")
        try:
            while self.job_status(job)['numberBatchesTotal'] != self.job_status(job)['numberBatchesCompleted']:
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        logging.info("SOQL ran successfully, fetching results")
        batch_id_list = [batch['id'] for batch in self.get_batch_list(job) if batch['state'] == 'Completed']
        return job, batch_id_list

    def fetch_batch_results(self, job: str, batch_id_list: List[str]) -> Iterator:
        for i, batch_id in enumerate(batch_id_list):
            logging.info(f"Fetching batch results for batch {i + 1}/{len(batch_id_list)}, id : {batch_id}")
            yield from self.get_all_results_from_query_batch(batch_id, job)
        self.close_job(job)

    def get_all_results_from_query_batch(self, batch_id: str, job_id: str = None, chunk_size: int = 8196) -> Iterator:
        """
        Gets result ids and generates each result set from the batch and returns it
        as a generator fetching the next result set when needed

        Args:
            batch_id: id of batch
            job_id: id of job, if not provided, it will be looked up
            chunk_size : size of chunks for stream
        """
        result_ids = self.get_query_batch_result_ids(batch_id, job_id=job_id)
        if not result_ids:
            raise RuntimeError('Batch is not complete')
        for result_id in result_ids:
            yield self.get_query_batch_result(
                batch_id,
                result_id,
                job_id=job_id,
                chunk_size=chunk_size
            )

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def get_query_batch_result(self, batch_id: str, result_id: str, job_id: str = None,
                               chunk_size: int = 8196) -> Iterator:
        job_id = job_id or self.lookup_job_id(batch_id)
        uri = urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result/{2}".format(
                job_id, batch_id, result_id),
        )

        try:
            resp = requests.get(uri, headers=self.headers(), stream=True)
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get query batch results, error: {e}") from e

        self.check_status(resp)
        iterator = (x.replace(b'\0', b'') for x in resp.iter_lines(chunk_size=chunk_size))
        return iterator

    def build_query_from_string(self, soql_query_string: str) -> SoqlQuery:
        try:
            soql_query = SoqlQuery.build_from_query_string(soql_query_string, self.describe_object)
        except SalesforceExpiredSession as expired_error:
            raise SalesforceClientException(expired_error) from expired_error
        return soql_query

    def build_soql_query_from_object_name(self, sf_object: str, fields: list = None) -> SoqlQuery:
        sf_object = sf_object.strip()
        try:
            soql_query = SoqlQuery.build_from_object(sf_object, self.describe_object, fields=fields)
        except SalesforceExpiredSession as expired_error:
            raise SalesforceClientException(expired_error) from expired_error
        except ValueError as e:
            raise SalesforceClientException(e) from e
        return soql_query

    def get_bulk_fetchable_objects(self):
        all_s_objects = self.simple_client.describe()["sobjects"]
        to_fetch = []
        # Only objects with the 'queryable' set to True and ones that are not in the OBJECTS_NOT_SUPPORTED_BY_BULK are
        # queryable by the Bulk API. This list might not be exact, and some edge-cases might have to be addressed.
        for sf_object in all_s_objects:
            if sf_object.get('queryable') and not sf_object.get('name') in OBJECTS_NOT_SUPPORTED_BY_BULK:
                to_fetch.append({"label": sf_object.get('label'), 'value': sf_object.get('name')})
        return to_fetch
