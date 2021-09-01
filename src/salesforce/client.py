import logging
from time import sleep
from urllib.parse import urlparse, urljoin

from retry import retry
import requests
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import SFType

from .soql_query import SoqlQuery

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64"]
CHUNK_SIZE = 100000
ALLOWED_CHUNKING_OBJECTS = ["account", "campaign", "campaignMember", "case", "contact", "lead", "loginhistory",
                            "opportunity", "task", "user"]


class SalesforceClient(SalesforceBulk):
    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 API_version=DEFAULT_API_VERSION, sandbox=False,
                 security_token=None, organizationId=None, client_id=None, domain=None):

        super().__init__(sessionId, host, username, password,
                         API_version, sandbox,
                         security_token, organizationId, client_id, domain)

        self.api_version = API_version
        self.host = urlparse(self.endpoint).hostname

    def describe_object(self, sf_object):
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)
        object_desc = salesforce_type.describe()
        field_names = [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]

        return field_names

    @staticmethod
    def is_bulk_supported_field(field):
        if field["type"] in NON_SUPPORTED_BULK_FIELD_TYPES:
            return False
        return True

    @retry(tries=3, delay=5)
    def run_query(self, soql_query):
        pk_chunking = False
        if soql_query.sf_object.lower() in ALLOWED_CHUNKING_OBJECTS:
            pk_chunking = CHUNK_SIZE

        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV', concurrency='Parallel',
                                       pk_chunking=pk_chunking)
        self.query(job, soql_query.query)
        logging.info(f"Running SOQL : {soql_query.query}")
        try:
            while not self.job_status(job)['numberBatchesTotal'] == self.job_status(job)['numberBatchesCompleted']:
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        logging.info("SOQL ran successfully, fetching results")
        batch_id_list = [batch['id'] for batch in self.get_batch_list(job) if batch['state'] == 'Completed']
        return job, batch_id_list

    def fetch_batch_results(self, job, batch_id_list):
        for batch_id in batch_id_list:
            for result in self.get_all_results_from_query_batch(batch_id, job):
                yield result
        self.close_job(job)

    def get_all_results_from_query_batch(self, batch_id, job_id=None, chunk_size=8196):
        """
        Gets result ids and generates each result set from the batch and returns it
        as an generator fetching the next result set when needed

        Args:
            batch_id: id of batch
            job_id: id of job, if not provided, it will be looked up
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

    def get_query_batch_result(self, batch_id, result_id, job_id=None, chunk_size=8196):
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

    def build_query_from_string(self, soql_query_string):
        soql_query = SoqlQuery.build_from_query_string(soql_query_string, self.describe_object)
        return soql_query

    def build_soql_query_from_object_name(self, sf_object):
        sf_object = sf_object.strip()
        soql_query = SoqlQuery.build_from_object(sf_object, self.describe_object)
        return soql_query
