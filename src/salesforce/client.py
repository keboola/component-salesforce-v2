import copy
import logging
import os
from collections import OrderedDict
from typing import Any, Iterator
from urllib.parse import urlparse

import backoff
from keboola.http_client import HttpClient
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


class SalesforceClientException(Exception):
    pass


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

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def describe_object(self, sf_object: str) -> list[str]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [field['name'] for field in object_desc['fields'] if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
    def describe_object_w_metadata(self, sf_object: str) -> list[tuple[str, str]]:
        salesforce_type = SFType(sf_object, self.sessionId, self.host, sf_version=self.api_version)

        try:
            object_desc = salesforce_type.describe()
        except ConnectionError as e:
            raise SalesforceClientException(f"Cannot get SalesForce object description, error: {e}.") from e

        return [(field['name'], field['type']) for field in object_desc['fields']
                if self.is_bulk_supported_field(field)]

    @backoff.on_exception(backoff.expo, SalesforceClientException, max_tries=3)
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

    def test_query(self, soql_query: SoqlQuery, add_limit: bool = False, include_deleted: bool = False) -> Iterator:
        """Test query has been implemented to prevent long timeouts of batched queries.
        
        Args:
            soql_query: The SOQL query to test
            add_limit: Whether to add a LIMIT clause to the query
            include_deleted: Whether to use queryAll endpoint for deleted/archived records
        
        Returns:
            Query result iterator
        """
        test_query = copy.deepcopy(soql_query)
        if add_limit:
            test_query.add_limit()
        
        query_length = len(test_query.query)
        logging.debug(f"Test query length: {query_length} characters")
        
        use_post = query_length > 1500
        endpoint = 'queryAll' if include_deleted else 'query'
        
        try:
            logging.info("Running test SOQL.")
            if use_post:
                logging.debug(f"Using POST method for {endpoint} due to query length ({query_length} chars)")
                result = self.simple_client.restful(endpoint, method='POST', json={'q': test_query.query})
            else:
                if include_deleted:
                    result = self.simple_client.query_all(test_query.query)
                else:
                    result = self.simple_client.query(test_query.query)
        except (SalesforceMalformedRequest, SalesforceClientException) as e:
            error_str = str(e)
            if ('414' in error_str or '431' in error_str) and not use_post:
                logging.warning(f"Query failed with {error_str}, retrying with POST method")
                try:
                    result = self.simple_client.restful(endpoint, method='POST', json={'q': test_query.query})
                except Exception as retry_error:
                    raise SalesforceClientException(
                        f"Test Query failed (length: {query_length} chars). Error: {retry_error}"
                    ) from retry_error
            else:
                raise SalesforceClientException(
                    f"Test Query failed (length: {query_length} chars). Error: {e}"
                ) from e

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
