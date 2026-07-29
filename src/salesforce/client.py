import copy
import logging
import os
from collections import OrderedDict
from typing import Any, Iterator
from urllib.parse import urlparse

import backoff
import requests
from keboola.http_client import HttpClient
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import ConnectionError as RequestsConnectionError
from simple_salesforce.api import Salesforce, SFType
from simple_salesforce.bulk2 import ColumnDelimiter, LineEnding, Operation, QueryResult, SFBulk2Type
from simple_salesforce.exceptions import SalesforceBulkV2LoadError, SalesforceExpiredSession, SalesforceMalformedRequest

from .soql_query import SoqlQuery

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

DEFAULT_QUERY_PAGE_SIZE = 50000

# default as previous versions of this component ex-salesforce-v2 had 40.0
DEFAULT_API_VERSION = "52.0"
MAX_RETRIES = 3

# urllib3 waits TRANSPORT_RETRY_BACKOFF_FACTOR * 2 ** (attempt - 1) seconds between transport retries.
TRANSPORT_RETRY_BACKOFF_FACTOR = 1


class SalesforceClientException(Exception):
    pass


# Retried by the describe_object* methods. They talk to Salesforce through SFType, which builds its own requests
# session, so _mount_transport_retries does not cover them; RequestsConnectionError is not a subclass of the
# built-in ConnectionError they already catch either, so a dropped connection would otherwise kill the whole job.
# After max_tries the original exception is re-raised unchanged.
RETRIED_DESCRIBE_ERRORS = (SalesforceClientException, RequestsConnectionError)


class SalesforceBulk2(SFBulk2Type):
    def __init__(self, sf_client, object_name: str):
        super().__init__(object_name, sf_client.bulk2_url, sf_client.headers, sf_client.session)

    def download(self,
                 query: str,
                 path: str,
                 max_records: int = DEFAULT_QUERY_PAGE_SIZE,
                 column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
                 line_ending: LineEnding = LineEnding.LF,
                 wait: int = 5, ) -> list[QueryResult]:

        if not os.path.exists(path):
            raise SalesforceBulkV2LoadError(f"Path does not exist: {path}")

        res = self._client.create_job(Operation.query_all, query, column_delimiter, line_ending)
        job_id = res["id"]
        self._client.wait_for_job(job_id, True, wait)

        results = []
        locator = "INIT"
        while locator:
            if locator == "INIT":
                locator = ""
            result = self._client.download_job_data(path, job_id, locator, max_records)
            locator = result["locator"]
            results.append(result)
        return results


class SalesforceClient(HttpClient):
    def __init__(self, simple_client: Salesforce, api_version: str,
                 consumer_key: str = None, consumer_secret: str = None) -> None:
        # Initialize the client with from_connected_app or from_security_token, this creates a login with the
        # simple salesforce client. The simple_client sessionId is a Bearer token that is result of the login.
        super().__init__('NONE', max_retries=MAX_RETRIES)
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self.simple_client = simple_client
        self.api_version = api_version
        self.host = urlparse(self.simple_client.base_url).hostname
        self.sessionId = self.simple_client.session_id
        self._mount_transport_retries(self.simple_client.session)

    @staticmethod
    def _mount_transport_retries(session: requests.Session) -> None:
        """Retries requests that never got a response because the connection was dropped.

        Salesforce occasionally closes a connection without answering, which reaches the component as
        requests.exceptions.ConnectionError("('Connection aborted.', RemoteDisconnected(...))") and kills the
        whole job. Only transport failures are retried, and only for the idempotent methods urllib3 allows by
        default. HTTP responses are never retried (status=0 and no status_forcelist), so every response the
        component already handled - including error responses - is passed through untouched. Once the retries
        are exhausted the original exception propagates exactly as before.
        """
        retry = Retry(
            total=None,
            connect=MAX_RETRIES,
            read=MAX_RETRIES,
            status=0,
            other=0,
            backoff_factor=TRANSPORT_RETRY_BACKOFF_FACTOR,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

    @classmethod
    def from_connected_app(cls, username: str, password: str, consumer_key: str, consumer_secret: str, sandbox: str,
                           api_version: str = DEFAULT_API_VERSION, domain: str = None):
        domain = 'test' if sandbox else domain

        simple_client = Salesforce(username=username, password=password, consumer_secret=consumer_secret,
                                   consumer_key=consumer_key,
                                   domain=domain, version=api_version)

        return cls(simple_client=simple_client, api_version=api_version)

    @classmethod
    def from_security_token(cls, username: str, password: str, security_token: str, sandbox: str, api_version: str,
                            domain: str = None):

        domain = 'test' if sandbox else domain
        simple_client = Salesforce(username=username, password=password, security_token=security_token,
                                   domain=domain, version=api_version)

        return cls(simple_client=simple_client, api_version=api_version)

    @classmethod
    def from_connected_app_oauth_cc(cls, consumer_key: str, consumer_secret: str, domain: str, api_version: str):

        simple_client = Salesforce(consumer_key=consumer_key, consumer_secret=consumer_secret, domain=domain,
                                   version=api_version)

        return cls(simple_client=simple_client, api_version=api_version)

    @backoff.on_exception(backoff.expo, RETRIED_DESCRIBE_ERRORS, max_tries=3)
    def describe_object(self, sf_object: str) -> list[str]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, RETRIED_DESCRIBE_ERRORS, max_tries=3)
    def describe_object_w_metadata(self, sf_object: str) -> list[tuple[str, str]]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [(field['name'], field['type']) for field in object_desc['fields']
                if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, RETRIED_DESCRIBE_ERRORS, max_tries=3)
    def describe_object_w_complete_metadata(self, sf_object: str) -> dict[str, Any]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return object_desc

    @staticmethod
    def is_bulk_supported_field(field: OrderedDict) -> bool:
        return field["type"] not in NON_SUPPORTED_BULK_FIELD_TYPES

    def download(self, soql_query: SoqlQuery, path: str, fail_on_error: bool = False,
                 query_page_size: int = DEFAULT_QUERY_PAGE_SIZE) -> list[QueryResult]:
        try:
            bulk2 = SalesforceBulk2(self.simple_client, soql_query.sf_object)

            logging.info(f"Running SOQL : {soql_query.query}")
            query_results = bulk2.download(soql_query.query, path, max_records=query_page_size)
            logging.info("SOQL ran successfully")

            return query_results
        except SalesforceBulkV2LoadError as e:
            if fail_on_error:
                raise SalesforceClientException(e)
            logging.exception(e)

    def test_query(self, soql_query: SoqlQuery, add_limit: bool = False) -> Iterator:
        """Test query has been implemented to prevent long timeouts of batched queries."""
        test_query = copy.deepcopy(soql_query)
        if add_limit:
            test_query.add_limit()
        try:
            logging.info("Running test SOQL.")
            result = self.simple_client.query(test_query.query)
        except (SalesforceMalformedRequest, SalesforceClientException):
            raise SalesforceClientException(f"Test Query {test_query.query} failed, please re-check the query.")

        logging.info("Test query has been successful.")
        return result

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
            if sf_object.get("queryable") and sf_object.get("name") not in OBJECTS_NOT_SUPPORTED_BY_BULK:
                to_fetch.append({"label": sf_object.get("label"), "value": sf_object.get("name")})
        return to_fetch
