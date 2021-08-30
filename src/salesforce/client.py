import logging
from time import sleep
from urllib.parse import urlparse

from retry import retry
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from salesforce_bulk.salesforce_bulk import DEFAULT_API_VERSION
from simple_salesforce import SFType

from salesforce.soql_query import SoqlQuery

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64"]
CHUNK_SIZE = 100000
ALLOWED_CHUNKING_OBJECTS = ["account", "campaign", "campaignMember", "case", "contact", "lead", "loginhistory",
                            "opportunity", "task", "user"]


# describe object of python client returns field values not supported by bulk api, they must
# be manually filtered out
# NON_SUPPORTED_BULK_FIELD_NAMES = ["IndividualId", "IqScore", "StockKeepingUnit", "OutOfOfficeMessage"]


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
            for result in self.get_all_results_for_query_batch(batch_id, job, chunk_size=CHUNK_SIZE):
                yield result
        self.close_job(job)

    def build_query_from_string(self, soql_query_string):
        soql_query = SoqlQuery.build_from_query_string(soql_query_string, self.describe_object)
        return soql_query

    def build_soql_query_from_object_name(self, sf_object):
        sf_object = sf_object.strip()
        soql_query = SoqlQuery.build_from_object(sf_object, self.describe_object)
        return soql_query
