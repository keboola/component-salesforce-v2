'''
Template Component main class.

'''
import csv
import logging
from datetime import datetime
from os import path, mkdir
import shutil

import unicodecsv
from keboola.component.base import ComponentBase, UserException
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from retry import retry
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from simple_salesforce.exceptions import SalesforceResourceNotFound

from salesforce.client import SalesforceClient, SalesforceClientException
from salesforce.soql_query import SoqlQuery
from typing import List
from typing import Dict
from typing import Iterator

# default as previous versions of this component ex-salesforce-v2 had 40.0
DEFAULT_API_VERSION = "42.0"

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_SANDBOX = "sandbox"
KEY_API_VERSION = "api_version"
KEY_OBJECT = "object"
KEY_QUERY_TYPE = "query_type_selector"
KEY_SOQL_QUERY = "soql_query"
KEY_IS_DELETED = "is_deleted"

KEY_BUCKET_NAME = "bucket_name"

KEY_LOADING_OPTIONS = "loading_options"
KEY_LOADING_OPTIONS_INCREMENTAL = "incremental"
KEY_LOADING_OPTIONS_INCREMENTAL_FIELD = "incremental_field"
KEY_LOADING_OPTIONS_INCREMENTAL_FETCH = "incremental_fetch"
KEY_LOADING_OPTIONS_PKEY = "pkey"

RECORDS_NOT_FOUND = ['Records not found for this query']

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_SECURITY_TOKEN, [KEY_SOQL_QUERY, KEY_OBJECT]]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})

        bucket_name = params.get(KEY_BUCKET_NAME, self.get_bucket_name())
        bucket_name = f"in.c-{bucket_name}"

        last_run = self.get_state_file().get("last_run")
        if not last_run:
            last_run = str(datetime(2000, 1, 1).strftime('%Y-%m-%dT%H:%M:%S.000Z'))

        prev_output_columns = self.get_state_file().get("prev_output_columns")

        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])
        incremental = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL, False)

        self.validate_incremental_settings(incremental, pkey)

        salesforce_client = self.get_salesforce_client(params)

        soql_query = self.build_soql_query(salesforce_client, params, last_run)

        self.validate_soql_query(soql_query, pkey)

        logging.info(f"Primary key : {pkey} set")

        table = self.create_out_table_definition(f'{soql_query.sf_object}',
                                                 primary_key=pkey,
                                                 incremental=incremental,
                                                 is_sliced=True,
                                                 destination=f'{bucket_name}.{soql_query.sf_object}')

        self.create_sliced_directory(table.full_path)
        batch_results = self.run_query(salesforce_client, soql_query)
        output_columns = []
        for index, result in enumerate(self.fetch_result(batch_results)):
            logging.info("Writing results")
            output_columns = self.write_results(result, table.full_path, index)
            logging.info("Results written")
            output_columns = self.normalize_column_names(output_columns)

        if not output_columns:
            if prev_output_columns:
                output_columns = prev_output_columns
            elif params.get(KEY_QUERY_TYPE) == "Object":
                output_columns = soql_query.sf_object_fields

        table.columns = output_columns

        if output_columns:
            self.write_tabledef_manifest(table)
            soql_timestamp = str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'))
            self.write_state_file({"last_run": soql_timestamp,
                                   "prev_output_columns": output_columns})
        else:
            shutil.rmtree(table.full_path)

    @staticmethod
    def validate_incremental_settings(incremental: bool, pkey: List[str]) -> None:
        if incremental and not pkey:
            raise UserException("Incremental load is set but no private key. Specify a private key in the "
                                "configuration parameters")

    @staticmethod
    def validate_soql_query(soql_query: SoqlQuery, pkey: List[str]) -> None:
        missing_keys = soql_query.check_pkey_in_query(pkey)
        if missing_keys:
            raise UserException(f"Private Keys {missing_keys} not in query, Add to SOQL query or check that it exists"
                                f" in the Salesforce object.")

    def get_salesforce_client(self, params: Dict) -> SalesforceClient:
        try:
            return self._login_to_salesforce(params)
        except SalesforceAuthenticationFailed:
            raise UserException("Authentication Failed : recheck your username, password, and security token ")

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def _login_to_salesforce(self, params: Dict) -> SalesforceClient:
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX),
                                API_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION))

    @staticmethod
    def create_sliced_directory(table_path: str) -> None:
        logging.info("Creating sliced file")
        if not path.isdir(table_path):
            mkdir(table_path)

    @retry(tries=3, delay=5)
    def fetch_result(self, batch_results: Iterator) -> Iterator:
        try:
            for i, result in enumerate(batch_results):
                yield result
        except BulkBatchFailed as bulk_err:
            raise UserException(
                "Invalid Query: Failed to process query. Check syntax, objects, and fields") from bulk_err
        except SalesforceClientException as sf_err:
            raise UserException() from sf_err

    @staticmethod
    def write_results(result: Iterator, table: str, index: int) -> List[str]:
        slice_path = path.join(table, str(index))
        fieldnames = []
        with open(slice_path, 'w+', newline='') as out:
            reader = unicodecsv.DictReader(result)
            if reader.fieldnames != RECORDS_NOT_FOUND:
                fieldnames = reader.fieldnames
                writer = csv.DictWriter(out, fieldnames=reader.fieldnames, lineterminator='\n', delimiter=',')
                writer.writerows(reader)
            else:
                logging.info("No records found using SOQL query")
        return fieldnames

    def build_soql_query(self, salesforce_client: SalesforceClient, params: Dict, last_run: str) -> SoqlQuery:
        try:
            return self._build_soql_query(salesforce_client, params, last_run)
        except (ValueError, TypeError) as query_error:
            raise UserException(query_error) from query_error

    @staticmethod
    def _build_soql_query(salesforce_client: SalesforceClient, params: Dict, last_run: str) -> SoqlQuery:
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        salesforce_object = params.get(KEY_OBJECT)
        soql_query_string = params.get(KEY_SOQL_QUERY)
        incremental = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL, False)
        incremental_field = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_FIELD)
        incremental_fetch = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_FETCH)
        is_deleted = params.get(KEY_IS_DELETED, False)
        query_type = params.get(KEY_QUERY_TYPE)

        if query_type == "Custom SOQL":
            try:
                soql_query = salesforce_client.build_query_from_string(soql_query_string)
            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(f"Custom SOQL could not be built : {salesforce_error}") from salesforce_error
            except SalesforceClientException as salesforce_error:
                raise UserException(salesforce_error) from salesforce_error
        elif query_type == "Object":
            try:
                soql_query = salesforce_client.build_soql_query_from_object_name(salesforce_object)
            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(f"Object type {salesforce_object} does not exist in Salesforce, "
                                    f"enter a valid object") from salesforce_error
            except SalesforceClientException as salesforce_error:
                raise UserException(salesforce_error) from salesforce_error
        else:
            raise UserException(f'Either {KEY_SOQL_QUERY} or {KEY_OBJECT} parameters must be specified.')

        if incremental and incremental_fetch and incremental_field and last_run:
            soql_query.set_query_to_incremental(incremental_field, last_run)
        elif incremental and incremental_fetch and not incremental_field:
            raise UserException("Incremental field is not specified, if you want to use incremental fetching, it must "
                                "specified.")

        soql_query.set_deleted_option_in_query(is_deleted)

        return soql_query

    @staticmethod
    def normalize_column_names(output_columns: List[str]) -> List[str]:
        header_normalizer = get_normalizer(strategy=NormalizerStrategy.DEFAULT, forbidden_sub="_")
        return header_normalizer.normalize_header(output_columns)

    def get_bucket_name(self) -> str:
        config_id = self.environment_variables.config_id
        if not config_id:
            config_id = "000000000"
        bucket_name = f"kds-team-ex-salesforce-v2-{config_id}"
        return bucket_name

    @staticmethod
    def run_query(salesforce_client: SalesforceClient, soql_query: SoqlQuery) -> Iterator:
        try:
            return salesforce_client.run_query(soql_query)
        except SalesforceClientException as sf_exc:
            raise UserException(sf_exc)


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
