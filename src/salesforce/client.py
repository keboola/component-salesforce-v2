import logging
from retry import retry
from time import sleep
from salesforce_bulk import SalesforceBulk
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce import SFType

from salesforce.soql_query import SoqlQuery

DEFAULT_API_VERSION = "40.0"

NON_SUPPORTED_BULK_FIELD_TYPES = ["address", "location", "base64", "reference"]


class SalesforceClient(SalesforceBulk):
    # copied from SalesforceBulk lib except host is saved for describe object
    def __init__(self, sessionId=None, host=None, username=None, password=None,
                 API_version=DEFAULT_API_VERSION, sandbox=False,
                 security_token=None, organizationId=None, client_id=None, domain=None):
        if not sessionId and not username:
            raise RuntimeError(
                "Must supply either sessionId/instance_url or username/password")
        if not sessionId:
            sessionId, host = SalesforceBulk.login_to_salesforce(
                username, password, sandbox=sandbox, security_token=security_token,
                organizationId=organizationId, API_version=API_version, client_id=client_id,
                domain=domain)

        if host[0:4] == 'http':
            self.endpoint = host
        else:
            self.endpoint = "https://" + host
        self.host = host
        self.endpoint += "/services/async/%s" % API_version
        self.sessionId = sessionId
        self.jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
        self.jobs = {}  # dict of job_id => job_id
        self.batches = {}  # dict of batch_id => job_id
        self.job_content_types = {}  # dict of job_id => contentType
        self.batch_statuses = {}
        self.API_version = API_version

    def describe_object(self, sf_object):
        salesforce_type = SFType(sf_object, self.sessionId, self.host)
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
        job = self.create_queryall_job(soql_query.sf_object, contentType='CSV')
        batch = self.query(job, soql_query.query)
        self.close_job(job)
        logging.info(f"Running SOQL : {soql_query.query}")
        try:
            while not self.is_batch_done(batch):
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        batch_result = self.get_all_results_for_query_batch(batch)
        return {"object": soql_query.sf_object, "result": batch_result}

    def build_query_from_string(self, soql_query_string):
        soql_query = SoqlQuery(query=soql_query_string)
        soql_query.field_names = SoqlQuery.get_fields_from_query(soql_query_string)
        soql_query.sf_object_fields = self.describe_object(soql_query.sf_object)
        return soql_query

    def build_soql_query_from_object_name(self, sf_object):
        sf_object = sf_object.strip()
        object_fields = self.describe_object(sf_object)
        soql_query = SoqlQuery(sf_object_fields=object_fields, sf_object=sf_object, field_names=object_fields)
        return soql_query
