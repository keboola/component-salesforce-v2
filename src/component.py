import csv
import logging
import shutil
from datetime import datetime
from datetime import timezone
from os import path, mkdir
import json

from typing import Dict
from typing import Iterator
from typing import List

import unicodecsv
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableMetadata, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from retry import retry
from salesforce_bulk.salesforce_bulk import BulkApiError
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from simple_salesforce.exceptions import SalesforceResourceNotFound, SalesforceError

from salesforce.client import SalesforceClient, SalesforceClientException
from salesforce.soql_query import SoqlQuery

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
KEY_FIELDS = 'fields'

KEY_ADVANCED_FETCHING_OPTIONS = "advanced_fetching_options"
KEY_FETCH_IN_CHUNKS = "fetch_in_chunks"
KEY_CHUNK_SIZE = "chunk_size"

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
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        start_run_time = str(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z'))

        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})

        bucket_name = params.get(KEY_BUCKET_NAME, self.get_bucket_name())
        bucket_name = f"in.c-{bucket_name}"

        statefile = self.get_state_file()

        last_run = statefile.get("last_run")
        if not last_run:
            last_run = str(datetime(2000, 1, 1).strftime('%Y-%m-%dT%H:%M:%S.000Z'))

        prev_output_columns = statefile.get("prev_output_columns")

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

        advanced_fetching_options = params.get(KEY_ADVANCED_FETCHING_OPTIONS, {})
        fetch_in_chunks = advanced_fetching_options.get(KEY_FETCH_IN_CHUNKS, False)
        output_columns = []

        if fetch_in_chunks:
            job_id, batch_ids = self.run_chunked_query(salesforce_client, soql_query)
            for index, result in enumerate(self.fetch_chunked_result(salesforce_client, job_id, batch_ids)):
                output_columns = self.write_results(result, table.full_path, index)
                output_columns = self.normalize_column_names(output_columns)
        else:
            batch_results = self.run_query(salesforce_client, soql_query)
            for index, result in enumerate(self.fetch_result(batch_results)):
                output_columns = self.write_results(result, table.full_path, index)
                output_columns = self.normalize_column_names(output_columns)

        if not output_columns:
            if prev_output_columns:
                output_columns = prev_output_columns
            elif params.get(KEY_QUERY_TYPE) == "Object":
                output_columns = soql_query.sf_object_fields

        table.columns = output_columns

        if output_columns:

            if params.get(KEY_QUERY_TYPE) == "Object":
                tm = self.store_table_metadata(salesforce_client, soql_query.sf_object, table)
                table.table_metadata = tm

            self.write_manifest(table)
            self.write_state_file({"last_run": start_run_time,
                                   "prev_output_columns": output_columns})
        else:
            shutil.rmtree(table.full_path)

    @staticmethod
    def store_table_metadata(salesforce_client, sf_object, table):
        description = None
        try:
            description = salesforce_client.describe_object_w_complete_metadata(sf_object)
            # formatted_json_string = json.dumps(description, indent=4)
        except SalesforceClientException as salesforce_error:
            logging.error(f"Cannot fetch metadata for object {sf_object}: {salesforce_error}")
        tm = TableMetadata(table.get_manifest_dictionary())

        if description:
            for item in description["fields"]:

                if salesforce_client.is_bulk_supported_field(item):
                    column_name = str(item["name"])
                    column_type = str(item["type"])
                    nullable = item["nillable"]
                    default = item["defaultValue"]

                    tm.add_column_data_type(column=column_name,
                                            data_type=SupportedDataTypes("STRING"),
                                            source_data_type=column_type,
                                            nullable=nullable,
                                            default=default)

                    tm.add_column_metadata(column_name, "sf_object", json.dumps(item))

            table_md = {str(k): str(v) for k, v in description.items() if k not in ["childRelationships", "fields"]}

            for key, value in table_md.items():
                if isinstance(value, list):
                    continue
                if isinstance(value, dict):
                    continue
                tm.add_table_metadata(key, value)

        return tm

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
        except SalesforceAuthenticationFailed as e:
            raise UserException("Authentication Failed : recheck your username, password, and security token ") from e

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def _login_to_salesforce(self, params: Dict) -> SalesforceClient:
        advanced_fetching_options = params.get(KEY_ADVANCED_FETCHING_OPTIONS, {})
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX),
                                API_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                                pk_chunking_size=advanced_fetching_options.get(KEY_CHUNK_SIZE))

    @staticmethod
    def create_sliced_directory(table_path: str) -> None:
        logging.info("Creating sliced file")
        if not path.isdir(table_path):
            mkdir(table_path)

    @retry(tries=3, delay=5)
    def fetch_chunked_result(self, salesforce_client, job_id, batch_ids) -> Iterator:
        try:
            yield from salesforce_client.fetch_batch_results(job_id, batch_ids)
        except BulkBatchFailed as bulk_err:
            raise UserException(
                "Invalid Query: Failed to process query. Check syntax, objects, and fields") from bulk_err
        except SalesforceClientException as sf_err:
            raise UserException() from sf_err

    @retry(tries=3, delay=5)
    def fetch_result(self, batch_results: Iterator) -> Iterator:
        try:
            yield from batch_results
        except BulkBatchFailed as bulk_err:
            raise UserException("Invalid Query: Failed to process query. "
                                "Check syntax, objects, and fields") from bulk_err

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
        fields = params.get(KEY_FIELDS, None)

        if query_type == "Custom SOQL":
            try:
                soql_query = salesforce_client.build_query_from_string(soql_query_string)
            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(f"Custom SOQL could not be built : {salesforce_error}") from salesforce_error
            except SalesforceClientException as salesforce_error:
                raise UserException(f"Cannot get Salesforce object description, error: {salesforce_error}") \
                    from salesforce_error

        elif query_type == "Object":
            try:
                soql_query = salesforce_client.build_soql_query_from_object_name(salesforce_object, fields)
                logging.info(f"Downloading salesforce object: {salesforce_object}.")

            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(f"Object type {salesforce_object} does not exist in Salesforce, "
                                    f"enter a valid object") from salesforce_error
            except SalesforceClientException as salesforce_error:
                error_message = str(salesforce_error)
                if 'INVALID_OPERATION_WITH_EXPIRED_PASSWORD' in error_message:
                    custom_message = "Your password has expired. Please reset your password or contact your " \
                                     "Salesforce Admin to reset the password and use the new one. You can also set " \
                                     "your Salesforce user for a password that never expires in the Password " \
                                     "Policies: https://help.salesforce.com/s/articleView?id=sf.admin_password.htm" \
                                     "&type=5. Please note that when the password is changed, " \
                                     "a new security token is generated."
                else:
                    custom_message = error_message
                raise UserException(custom_message) from salesforce_error

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
    def run_chunked_query(salesforce_client: SalesforceClient, soql_query: SoqlQuery) -> Iterator:
        try:
            return salesforce_client.run_chunked_query(soql_query)
        except SalesforceClientException as sf_exc:
            raise UserException(sf_exc) from sf_exc
        except BulkApiError as sf_exc:
            raise UserException(sf_exc) from sf_exc

    @sync_action('testConnection')
    def test_connection(self):
        """
        Tries to log into Salesforce, raises user exception if login params ar incorrect

        """
        params = self.configuration.parameters
        self.get_salesforce_client(params)

    @sync_action('loadObjects')
    def load_possible_objects(self) -> List[SelectElement]:
        """
        Finds all possible objects in Salesforce that can be fetched by the Bulk API

        Returns: a List of dictionaries containing 'name' and 'value' of the SF object, where 'name' is the Label name/
        readable name of the object, and 'value' is the name of the object you can use to query the object

        """
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        return [SelectElement(**c) for c in salesforce_client.get_bulk_fetchable_objects()]

    @sync_action("loadFields")
    def load_fields(self) -> List[SelectElement]:
        """Returns fields available for selected object."""
        params = self.configuration.parameters
        object_name = params.get("object")
        salesforce_client = self.get_salesforce_client(params)
        descriptions = salesforce_client.describe_object_w_metadata(object_name)
        return [SelectElement(label=f'{field[0]} ({field[1]})', value=field[0]) for field in descriptions]

    @sync_action("loadPossibleIncrementalField")
    def load_possible_incremental_field(self) -> List[SelectElement]:
        """
        Gets all possible fields of a Salesforce object. It determines the name of the SF object either from the input
        object name or from the SOQL query. This data is used to select an incremental field

        Returns: a List of dictionaries containing 'name' and 'value' of each field of the SF object,
        where 'name' is the Label name/ readable name of the field, and 'value' is the exact name of the field that can
        be used in an SOQL query

        """
        params = self.configuration.parameters
        if params.get(KEY_QUERY_TYPE) == "Custom SOQL":
            object_name = self._get_object_name_from_custom_query()
        else:
            object_name = params.get(KEY_OBJECT)
        return self._get_object_fields_names_and_values(object_name)

    @sync_action("loadPossiblePrimaryKeys")
    def load_possible_primary_keys(self) -> List[SelectElement]:
        """
        Gets all possible primary keys of the data returned of a saleforce object. If the exact object is specified,
        each field is returned, if a query is specified, it is run with LIMIT 1 and the returned data is analyzed to
        determine the fieldnames of the final table.

        Returns: a List of dictionaries containing 'name' and 'value' of each field of the SF object or each field that
        is returned by a custom SOQL query. 'name' is the Label name/ readable name of the field,
        and 'value' is the name of the field in storage

        """
        params = self.configuration.parameters
        if params.get(KEY_QUERY_TYPE) == "Custom SOQL":
            return self._get_object_fields_from_query()
        elif params.get(KEY_QUERY_TYPE) == "Object":
            object_name = params.get(KEY_OBJECT)
            return self._get_object_fields_names_and_normalized_values(object_name)
        else:
            raise UserException(f"Invalid {KEY_QUERY_TYPE}")

    def _get_object_name_from_custom_query(self) -> str:
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        query = self.build_soql_query(salesforce_client, params, None)
        return query.sf_object

    def _get_object_fields_names_and_normalized_values(self, object_name: str) -> List[SelectElement]:
        """
        Return object fields for sync action

        Args:
            object_name:

        Returns:

        """
        columns = self._get_fields_of_object_by_name(object_name)
        column_values = self.normalize_column_names(columns)
        return [SelectElement(label=column, value=column_values[i]) for i, column in enumerate(columns)]

    def _get_object_fields_names_and_values(self, object_name: str) -> List[SelectElement]:
        columns = self._get_fields_of_object_by_name(object_name)
        return [SelectElement(label=column, value=column) for column in columns]

    def _get_fields_of_object_by_name(self, object_name: str) -> List[str]:
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        return salesforce_client.describe_object(object_name)

    def _get_object_fields_from_query(self) -> List[SelectElement]:
        """
        Return object fields for sync action
        Returns:

        """
        result = self._get_first_result_from_custom_soql()

        if not result:
            raise UserException("Failed to determine fields from SOQL query, "
                                "make sure the SOQL query is valid and that it returns data.")

        columns = list(result.keys())
        if "attributes" in columns:
            columns.remove("attributes")
        column_values = self.normalize_column_names(columns)

        return [SelectElement(label=column, value=column_values[i]) for i, column in enumerate(columns)]

    def _get_first_result_from_custom_soql(self) -> Dict:
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        query = params.get(KEY_SOQL_QUERY)
        if " limit " not in query.lower():
            query = f"{query} LIMIT 1"
        else:
            # LIMIT statement should always come at the end
            limit_location = query.lower().find(" limit ")
            query = f"{query[:limit_location]} LIMIT 1"
        try:
            result = salesforce_client.simple_client.query(query)
        except SalesforceError as e:
            raise UserException("Failed to determine fields from SOQL query, make sure the SOQL query is valid") from e

        return result.get("records")[0] if result.get("totalSize") == 1 else None

    @staticmethod
    def run_query(salesforce_client: SalesforceClient, soql_query: SoqlQuery) -> Iterator:
        try:
            return salesforce_client.run_query(soql_query)
        except SalesforceClientException as sf_exc:
            raise UserException(sf_exc) from sf_exc


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
