import logging
from time import sleep
from urllib.parse import urlparse, urljoin

from retry import retry
import requests
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce.exceptions import SalesforceExpiredSession
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import SFType

from collections import OrderedDict
from .soql_query import SoqlQuery
from typing import List
from typing import Any
from typing import Optional
from typing import Iterator

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64"]
CHUNK_SIZE = 100000


class SalesforceClientException(Exception):
    pass


class SalesforceClient(SalesforceBulk):
    def __init__(self, sessionId: Optional[Any] = None, host: Optional[Any] = None, username: str = None,
                 password: str = None,
                 API_version: str = DEFAULT_API_VERSION, sandbox: bool = False,
                 security_token: str = None, organizationId: Optional[Any] = None, client_id: Optional[Any] = None,
                 domain: Optional[Any] = None) -> None:

        super().__init__(sessionId, host, username, password,
                         API_version, sandbox,
                         security_token, organizationId, client_id, domain)

        self.api_version = API_version
        self.host = urlparse(self.endpoint).hostname

    @retry(ConnectionError, tries=3, delay=5)
    def describe_object(self, sf_object: str) -> List[str]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)
        object_desc = salesforce_type.describe()
        field_names = [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]
        return field_names

    @staticmethod
    def is_bulk_supported_field(field: OrderedDict) -> bool:
        if field["type"] in NON_SUPPORTED_BULK_FIELD_TYPES:
            return False
        return True

    @retry(ConnectionError, tries=3, delay=5)
    def run_query(self, soql_query: SoqlQuery) -> Iterator:
        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV', concurrency='Parallel')
        batch = self.query(job, soql_query.query)
        logging.info(f"Running SOQL : {soql_query.query}")
        try:
            while not self.is_batch_done(batch):
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        except SalesforceExpiredSession as expired_error:
            raise SalesforceClientException(expired_error) from expired_error

        logging.info("SOQL ran successfully, fetching results")
        batch_result = self.get_all_results_from_query_batch(batch)
        return batch_result

    def fetch_batch_results(self, job: str, batch_id_list: List[str]) -> Iterator:
        for batch_id in batch_id_list:
            for result in self.get_all_results_from_query_batch(batch_id, job):
                yield result
        self.close_job(job)

    @retry(ConnectionError, tries=5, delay=5)
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
            try:
                yield self.get_query_batch_result(
                    batch_id,
                    result_id,
                    job_id=job_id,
                    chunk_size=chunk_size
                )
            except ConnectionError as conn_err:
                raise SalesforceClientException(
                    "Failed to get batch as salesforce aborted the connection") from conn_err

    @retry(ConnectionError, tries=5, delay=10)
    def get_query_batch_result(self, batch_id: str, result_id: str, job_id: str = None,
                               chunk_size: int = 8196) -> Iterator:
        job_id = job_id or self.lookup_job_id(batch_id)

        uri = urljoin(
            self.endpoint + "/",
            "job/{0}/batch/{1}/result/{2}".format(
                job_id, batch_id, result_id),
        )

        resp = requests.get(uri, headers=self.headers(), stream=True)
        self.check_status(resp)

        iterator = (x.replace(b'\0', b'') for x in resp.iter_lines(chunk_size=chunk_size))
        return iterator

    def build_query_from_string(self, soql_query_string: str) -> SoqlQuery:
        try:
            soql_query = SoqlQuery.build_from_query_string(soql_query_string, self.describe_object)
        except SalesforceExpiredSession as expired_error:
            raise SalesforceClientException(expired_error) from expired_error
        return soql_query

    def build_soql_query_from_object_name(self, sf_object: str) -> SoqlQuery:
        sf_object = sf_object.strip()
        try:
            soql_query = SoqlQuery.build_from_object(sf_object, self.describe_object)
        except SalesforceExpiredSession as expired_error:
            raise SalesforceClientException(expired_error) from expired_error
        return soql_query
