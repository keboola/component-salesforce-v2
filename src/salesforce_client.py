import logging
from salesforce_bulk import SalesforceBulk
from simple_salesforce import SFType
from simple_salesforce.exceptions import SalesforceResourceNotFound

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
        field_names = []
        try:
            object_desc = salesforce_type.describe()
            field_names = [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]
        except SalesforceResourceNotFound:
            logging.exception(f"Object type {sf_object} does not exist in Salesforce, enter a valid object")
        return field_names

    @staticmethod
    def is_bulk_supported_field(field):
        if field["type"] in NON_SUPPORTED_BULK_FIELD_TYPES:
            return False
        return True
