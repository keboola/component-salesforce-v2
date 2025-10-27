import csv
import logging
import os
import shutil
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from enum import Enum
from os import mkdir, path

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import SupportedDataTypes, BaseType, ColumnDefinition
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType, SelectElement, ValidationResult
from keboola.utils.header_normalizer import NormalizerStrategy, get_normalizer
from retry import retry
from simple_salesforce.exceptions import SalesforceAuthenticationFailed, SalesforceError, SalesforceResourceNotFound

from salesforce.client import DEFAULT_API_VERSION, SalesforceClient, SalesforceClientException
from salesforce.soql_query import SoqlQuery

KEY_LOGIN_METHOD = "login_method"
KEY_CONSUMER_KEY = "#consumer_key"
KEY_CONSUMER_SECRET = "#consumer_secret"
KEY_DOMAIN = "domain"
KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_SANDBOX = "sandbox"
KEY_API_VERSION = "api_version"
KEY_OBJECT = "object"
KEY_QUERY_TYPE = "query_type_selector"
KEY_SOQL_QUERY = "soql_query"
KEY_IS_DELETED = "is_deleted"
KEY_FIELDS = "fields"

KEY_BUCKET_NAME = "bucket_name"
KEY_OUTPUT_TABLE_NAME = "output_table_name"

KEY_LOADING_OPTIONS = "loading_options"
KEY_LOADING_OPTIONS_INCREMENTAL = "incremental"
KEY_LOADING_OPTIONS_INCREMENTAL_FIELD = "incremental_field"
KEY_LOADING_OPTIONS_INCREMENTAL_FETCH = "incremental_fetch"
KEY_LOADING_OPTIONS_INCREMENTAL_OVERLAP_SECONDS = "incremental_overlap_seconds"
KEY_LOADING_OPTIONS_PKEY = "pkey"

# Proxy
KEY_PROXY = "proxy"
KEY_USE_PROXY = "use_proxy"
KEY_PROXY_SERVER = "proxy_server"
KEY_PROXY_PORT = "proxy_port"
KEY_PROXY_USERNAME = "username"
KEY_PROXY_PASSWORD = "#password"
KEY_USE_HTTP_PROXY_AS_HTTPS = "use_http_proxy_as_https"

RECORDS_NOT_FOUND = ["Records not found for this query"]

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [[KEY_SOQL_QUERY, KEY_OBJECT]]
REQUIRED_IMAGE_PARS = []

DEFAULT_LOGIN_METHOD = "security_token"


class LoginType(str, Enum):
    SECURITY_TOKEN_LOGIN = "security_token"
    CONNECTED_APP_LOGIN = "connected_app"
    CONNECTED_APP_OAUTH_CC = "connected_app_oauth_cc"

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def ordereddict_to_dict(value):
    if isinstance(value, OrderedDict):
        return {k: ordereddict_to_dict(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [ordereddict_to_dict(item) for item in value]
    elif isinstance(value, dict):
        return {k: ordereddict_to_dict(v) for k, v in value.items()}
    else:
        return value


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        start_run_time = str(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"))

        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})

        bucket_name = params.get(KEY_BUCKET_NAME, self.get_bucket_name())
        bucket_name = f"in.c-{bucket_name}"

        state_file = self.get_state_file()

        last_run = state_file.get("last_run")
        if not last_run:
            last_run = str(datetime(2000, 1, 1).strftime("%Y-%m-%dT%H:%M:%S.000Z"))

        prev_output_columns = state_file.get("prev_output_columns")

        pkey = loading_options.get(KEY_LOADING_OPTIONS_PKEY, [])
        incremental = bool(loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL))

        self.validate_incremental_settings(incremental, pkey)

        salesforce_client = self.get_salesforce_client(params)
        soql_query = self.build_soql_query(salesforce_client, params, last_run)
        self.validate_soql_query(soql_query, pkey)
        logging.info(f"Primary key : {pkey} set")

        table_name = loading_options.get(KEY_OUTPUT_TABLE_NAME, False) or soql_query.sf_object
        table = self.create_out_table_definition(
            table_name, incremental=incremental, is_sliced=True, destination=f"{bucket_name}.{table_name}"
        )
        self.create_sliced_directory(table.full_path)

        is_deleted = params.get(KEY_IS_DELETED, False)
        self._test_query(salesforce_client, soql_query, True, is_deleted)

        results = salesforce_client.download(soql_query, table.full_path)
        logging.info(f"Downloaded {len(results)} files")
        total_records = sum(result.get("number_of_records", 0) for result in results)
        logging.debug([result for result in results])
        logging.info(f"Downloaded {total_records} records in total")

        # remove headers and get columns
        output_columns = self._fix_header_from_csv(results)
        output_columns = self.normalize_column_names(output_columns)

        if not output_columns:
            if prev_output_columns:
                output_columns = prev_output_columns
            elif params.get(KEY_QUERY_TYPE) == "Object":
                output_columns = soql_query.sf_object_fields

        if output_columns:
            query_type = params.get(KEY_QUERY_TYPE)

            table.schema = self._get_schema(salesforce_client, query_type, soql_query.sf_object, output_columns, pkey)

            self.write_manifest(table)
            self.write_state_file({"last_run": start_run_time, "prev_output_columns": output_columns})
        else:
            shutil.rmtree(table.full_path)

    @staticmethod
    def _fix_header_from_csv(results: list[dict]) -> list[str]:
        expected_header = None
        for result in results:
            result_file_path = result.get("file")
            temp_file_path = f"{result_file_path}.tmp"
            with (
                open(result_file_path, "r", encoding="utf-8") as infile,
                open(temp_file_path, "w", newline="", encoding="utf-8") as outfile,
            ):
                reader = csv.reader(infile)
                writer = csv.writer(outfile)
                # check if header is same as in other files
                actual_header = next(reader)  # Also skip the header
                if expected_header:
                    if actual_header != expected_header:
                        raise UserException(
                            f"Header in file {result_file_path} is different from expected. "
                            f"Expected: {expected_header}, Actual: {actual_header}"
                        )
                else:
                    expected_header = actual_header
                for row in reader:
                    writer.writerow(row)
            os.replace(temp_file_path, result_file_path)
        return expected_header

    def set_proxy(self) -> None:
        """Sets proxy if defined"""
        proxy_config = self.configuration.parameters.get(KEY_PROXY, {})
        if proxy_config.get(KEY_USE_PROXY):
            self._set_proxy(proxy_config)

    def _set_proxy(self, proxy_config: dict) -> None:
        """
        Sets proxy using environmental variables.
        Also, a special case when http proxy is used for https is handled by using KEY_USE_HTTP_PROXY_AS_HTTPS.
        os.environ['HTTPS_PROXY'] = (username:password@)your.proxy.server.com(:port)
        """
        proxy_server = proxy_config.get(KEY_PROXY_SERVER)
        proxy_port = str(proxy_config.get(KEY_PROXY_PORT))
        proxy_username = proxy_config.get(KEY_PROXY_USERNAME)
        proxy_password = proxy_config.get(KEY_PROXY_PASSWORD)
        use_http_proxy_as_https = proxy_config.get(
            KEY_USE_HTTP_PROXY_AS_HTTPS
        ) or self.configuration.image_parameters.get(KEY_USE_HTTP_PROXY_AS_HTTPS)

        if not proxy_server:
            raise UserException("You have selected use_proxy parameter, but you have not specified proxy server.")
        if not proxy_port:
            raise UserException("You have selected use_proxy parameter, but you have not specified proxy port.")

        _proxy_credentials = f"{proxy_username}:{proxy_password}@" if proxy_username and proxy_password else ""
        _proxy_server = f"{_proxy_credentials}{proxy_server}:{proxy_port}"

        if use_http_proxy_as_https:
            # This is a case of http proxy which also supports https.
            _proxy_server = f"http://{_proxy_server}"
        else:
            _proxy_server = f"https://{_proxy_server}"

        os.environ["HTTPS_PROXY"] = _proxy_server

        logging.info("Component will use proxy server.")

    @staticmethod
    def _test_query(salesforce_client, soql_query, add_limit: bool = False, include_deleted: bool = False):
        try:
            result = salesforce_client.test_query(soql_query=soql_query, add_limit=add_limit, include_deleted=include_deleted)
            return result
        except SalesforceClientException as e:
            raise UserException(e) from e

    @staticmethod
    def get_description(salesforce_client, sf_object):
        try:
            return salesforce_client.describe_object_w_complete_metadata(sf_object)
        except SalesforceClientException as salesforce_error:
            logging.error(f"Cannot fetch metadata for object {sf_object}: {salesforce_error}")
            return None

    def _add_columns_to_table_metadata(self, tm, description, output_columns):
        for item in description["fields"]:
            if item.get("name", "") in output_columns:
                column_name = str(item["name"])
                column_type = str(item["type"])
                nullable = item["nillable"]
                default = item["defaultValue"]
                label = item["label"]

                tm.add_column_data_type(
                    column=column_name,
                    data_type=self.convert_to_kbc_basetype(column_type),
                    source_data_type=column_type,
                    nullable=nullable,
                    default=default,
                )

                # The following is disabled since it caused exceeded metadata size
                # tm.add_column_metadata(column_name, "source_metadata", json.dumps(item))

                tm.add_column_descriptions({column_name: label})

    @staticmethod
    def add_table_metadata(tm, description):
        def recursive_flatten(prefix, nested_value):
            if isinstance(nested_value, dict):
                for k, v in nested_value.items():
                    recursive_flatten(f"{prefix}_{k}", v)
            elif isinstance(nested_value, list):
                for i, v in enumerate(nested_value):
                    recursive_flatten(f"{prefix}_{i}", v)
            else:
                tm.add_table_metadata(prefix, str(nested_value))

        table_md = {str(k): v for k, v in description.items() if k != "fields"}
        for key, value in table_md.items():
            value = ordereddict_to_dict(value)
            recursive_flatten(key, value)

    def _get_schema(self, salesforce_client, query_type, sf_object, output_columns, pkey):
        if query_type == "Object":
            fields_all = self.get_description(salesforce_client, sf_object).get("fields")
            fields = {field["name"]: field for field in fields_all if field["name"] in output_columns}
        else:
            fields = []

        schema = OrderedDict()
        for col in output_columns:
            if col in fields:
                field = fields[col]
                schema[col] = ColumnDefinition(
                    data_types=BaseType(dtype=self.convert_to_kbc_basetype(field.get("type"))),
                    nullable=field.get("nillable"),
                    description=field.get("label"),
                    primary_key=col in pkey,
                )
            else:
                schema[col] = ColumnDefinition(
                    data_types=BaseType(dtype=SupportedDataTypes.STRING), primary_key=col in pkey
                )

        return schema

    @staticmethod
    def convert_to_kbc_basetype(source_type: str) -> SupportedDataTypes:
        dtypes_mapping = {
            "id": SupportedDataTypes.STRING,
            "boolean": SupportedDataTypes.BOOLEAN,
            "reference": SupportedDataTypes.STRING,
            "string": SupportedDataTypes.STRING,
            "picklist": SupportedDataTypes.STRING,
            "textarea": SupportedDataTypes.STRING,
            "double": SupportedDataTypes.FLOAT,
            "phone": SupportedDataTypes.STRING,
            "email": SupportedDataTypes.STRING,
            "date": SupportedDataTypes.DATE,
            "datetime": SupportedDataTypes.TIMESTAMP,
            "url": SupportedDataTypes.STRING,
            "int": SupportedDataTypes.INTEGER,
            "currency": SupportedDataTypes.STRING,
            "multipicklist": SupportedDataTypes.STRING,
        }

        if source_type in dtypes_mapping:
            return SupportedDataTypes[dtypes_mapping[source_type]].value
        else:
            logging.warning(f"Unknown source type: {source_type}. Casting it to STRING.")
            return SupportedDataTypes.STRING

    @staticmethod
    def validate_incremental_settings(incremental: bool, pkey: list[str]) -> None:
        if incremental and not pkey:
            raise UserException(
                "Incremental load is set but no private key. Specify a private key in the configuration parameters"
            )

    @staticmethod
    def validate_soql_query(soql_query: SoqlQuery, pkey: list[str]) -> None:
        logging.info("Validating SOQL query")
        missing_keys = soql_query.check_pkey_in_query(pkey)
        if missing_keys:
            raise UserException(
                f"Private Keys {missing_keys} not in query, Add to SOQL query or check that it exists"
                f" in the Salesforce object."
            )

    def get_salesforce_client(self, params: dict) -> SalesforceClient:
        self.set_proxy()
        try:
            logging.info("Logging in to Salesforce")
            return self._login_to_salesforce(params)
        except SalesforceAuthenticationFailed as e:
            raise UserException(f"Authentication Failed : recheck your authorization parameters : {e}") from e

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def _login_to_salesforce(self, params: dict) -> SalesforceClient:
        login_method = self._get_login_method()

        if login_method == LoginType.SECURITY_TOKEN_LOGIN:
            if not params.get(KEY_SECURITY_TOKEN):
                raise UserException(
                    "Missing Required Parameter: Security Token. It is required when using Security Token Login"
                )

            if not params.get(KEY_USERNAME) or not params.get(KEY_PASSWORD):
                raise UserException(
                    "Missing Required Parameter: Both username and password are required for Security Token Login."
                )

            return SalesforceClient.from_security_token(
                username=params.get(KEY_USERNAME),
                password=params.get(KEY_PASSWORD),
                security_token=params.get(KEY_SECURITY_TOKEN),
                sandbox=params.get(KEY_SANDBOX),
                api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
            )

        elif login_method == LoginType.CONNECTED_APP_LOGIN:
            if not params.get(KEY_CONSUMER_KEY) or not params.get(KEY_CONSUMER_SECRET):
                raise UserException(
                    "Missing Required Parameter: At least one of Consumer Key and Consumer Secret "
                    "are missing. They are both required when using Connected App Login"
                )

            if not params.get(KEY_USERNAME) or not params.get(KEY_PASSWORD):
                raise UserException(
                    "Missing Required Parameter: Both username and password are required for Connected App Login."
                )

            return SalesforceClient.from_connected_app(
                username=params.get(KEY_USERNAME),
                password=params.get(KEY_PASSWORD),
                consumer_key=params.get(KEY_CONSUMER_KEY),
                consumer_secret=params.get(KEY_CONSUMER_SECRET),
                sandbox=params.get(KEY_SANDBOX),
                api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
            )

        elif login_method == LoginType.CONNECTED_APP_OAUTH_CC:
            if not params.get(KEY_CONSUMER_KEY) or not params.get(KEY_CONSUMER_SECRET):
                raise UserException(
                    "Missing Required Parameter: At least one of Consumer Key and Consumer Secret "
                    "are missing. They are both required when using Connected App Login"
                )

            if not (domain := params.get(KEY_DOMAIN)):
                raise UserException("Parameter 'domain' is needed for Client Credentials Flow. ")
            domain = self.process_salesforce_domain(domain)

            return SalesforceClient.from_connected_app_oauth_cc(
                consumer_key=params[KEY_CONSUMER_KEY],
                consumer_secret=params[KEY_CONSUMER_SECRET],
                api_version=params.get(KEY_API_VERSION, DEFAULT_API_VERSION),
                domain=domain,
            )

    def _get_login_method(self) -> LoginType:
        login_type_name = self.configuration.parameters.get(KEY_LOGIN_METHOD, DEFAULT_LOGIN_METHOD)
        try:
            return LoginType(login_type_name)
        except ValueError as val_err:
            raise UserException(
                f"'{login_type_name}' is not a valid Login Type. Enter one of : {LoginType.list()}"
            ) from val_err

    @staticmethod
    def process_salesforce_domain(url):
        if url.startswith("http://"):
            url = url[len("http://"):]
        if url.startswith("https://"):
            url = url[len("https://"):]
        if url.endswith(".salesforce.com"):
            url = url[: -len(".salesforce.com")]

        logging.debug(f"The component will use {url} for Client Credentials Flow type authentication.")

        return url

    @staticmethod
    def create_sliced_directory(table_path: str) -> None:
        logging.info("Creating sliced directory")
        if not path.isdir(table_path):
            mkdir(table_path)

    def build_soql_query(self, salesforce_client: SalesforceClient, params: dict, last_run: str = "") -> SoqlQuery:
        try:
            logging.info("Building SOQL query")
            return self._build_soql_query(salesforce_client, params, last_run)
        except (ValueError, TypeError) as query_error:
            raise UserException(query_error) from query_error

    @staticmethod
    def _build_soql_query(salesforce_client: SalesforceClient, params: dict, last_run: str) -> SoqlQuery:
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        salesforce_object = params.get(KEY_OBJECT)
        soql_query_string = params.get(KEY_SOQL_QUERY)
        incremental = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL, False)
        incremental_field = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_FIELD)
        incremental_fetch = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_FETCH)
        incremental_overlap_seconds = loading_options.get(KEY_LOADING_OPTIONS_INCREMENTAL_OVERLAP_SECONDS, 0)
        is_deleted = params.get(KEY_IS_DELETED, False)
        query_type = params.get(KEY_QUERY_TYPE)
        fields = params.get(KEY_FIELDS, None)

        if query_type == "Custom SOQL":
            try:
                soql_query = salesforce_client.build_query_from_string(soql_query_string)
            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(f"Custom SOQL could not be built : {salesforce_error}") from salesforce_error
            except SalesforceClientException as salesforce_error:
                raise UserException(
                    f"Cannot get Salesforce object description, error: {salesforce_error}"
                ) from salesforce_error

        elif query_type == "Object":
            try:
                soql_query = salesforce_client.build_soql_query_from_object_name(salesforce_object, fields)
                logging.info(f"Downloading salesforce object: {salesforce_object}.")

            except SalesforceResourceNotFound as salesforce_error:
                raise UserException(
                    f"Object type {salesforce_object} does not exist in Salesforce, enter a valid object"
                ) from salesforce_error
            except SalesforceClientException as salesforce_error:
                error_message = str(salesforce_error)
                if "INVALID_OPERATION_WITH_EXPIRED_PASSWORD" in error_message:
                    custom_message = (
                        "Your password has expired. Please reset your password or contact your "
                        "Salesforce Admin to reset the password and use the new one. You can also set "
                        "your Salesforce user for a password that never expires in the Password "
                        "Policies: https://help.salesforce.com/s/articleView?id=sf.admin_password.htm"
                        "&type=5. Please note that when the password is changed, "
                        "a new security token is generated."
                    )
                else:
                    custom_message = error_message
                raise UserException(custom_message) from salesforce_error

        else:
            raise UserException(f"Either {KEY_SOQL_QUERY} or {KEY_OBJECT} parameters must be specified.")

        if incremental and incremental_fetch and incremental_field and last_run:
            adjusted_last_run = last_run
            if incremental_overlap_seconds:
                if incremental_overlap_seconds < 0:
                    logging.warning(
                        f"Invalid incremental_overlap_seconds value: {incremental_overlap_seconds}. "
                        f"Value must be non-negative. Using 0 (no overlap)."
                    )
                elif incremental_overlap_seconds > 0:
                    try:
                        last_run_dt = datetime.strptime(last_run, "%Y-%m-%dT%H:%M:%S.%fZ")
                        adjusted_last_run_dt = last_run_dt - timedelta(seconds=incremental_overlap_seconds)
                        adjusted_last_run = adjusted_last_run_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
                        logging.info(
                            f"Applying incremental overlap of {incremental_overlap_seconds} seconds. "
                            f"Original watermark: {last_run}, Adjusted watermark: {adjusted_last_run}"
                        )
                    except ValueError as e:
                        logging.warning(f"Could not parse last_run timestamp '{last_run}': {e}. Using original value.")
                        adjusted_last_run = last_run
            soql_query.set_query_to_incremental(incremental_field, adjusted_last_run)
        elif incremental and incremental_fetch and not incremental_field:
            raise UserException(
                "Incremental field is not specified, if you want to use incremental fetching, it must specified."
            )

        soql_query.set_deleted_option_in_query(is_deleted)

        return soql_query

    @staticmethod
    def normalize_column_names(output_columns: list[str]) -> list[str]:
        header_normalizer = get_normalizer(strategy=NormalizerStrategy.DEFAULT, forbidden_sub="_")
        return header_normalizer.normalize_header(output_columns)

    def get_bucket_name(self) -> str:
        config_id = self.environment_variables.config_id
        if not config_id:
            config_id = "000000000"
        bucket_name = f"kds-team-ex-salesforce-v2-{config_id}"
        return bucket_name

    @sync_action("testConnection")
    def test_connection(self):
        """
        Tries to log into Salesforce, raises user exception if login params ar incorrect

        """
        params = self.configuration.parameters
        self.get_salesforce_client(params)

    @staticmethod
    def create_markdown_table(data):
        if not data:
            return ""
        headers = list(data[0].keys())
        table = "| " + " | ".join(headers) + " |\n"
        table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in data:
            row_values = [str(row[header]) for header in headers]
            table += "| " + " | ".join(row_values) + " |\n"
        return table

    @staticmethod
    def parse_result(data: OrderedDict) -> dict:
        return dict((k, v) for k, v in data.items() if k != "attributes")

    @sync_action("testQuery")
    def test_query(self):
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        soql_query_string = params.get(KEY_SOQL_QUERY)
        soql_query = salesforce_client.build_query_from_string(soql_query_string)
        self.validate_soql_query(soql_query, [])
        data = []
        try:
            result = self._test_query(salesforce_client, soql_query, False)
            if not result:
                return ValidationResult("Query returned no results", MessageType.WARNING)
            for _, result in enumerate(result.get("records", [])):
                data.append(self.parse_result(result))
            markdown = self.create_markdown_table(data)
            return ValidationResult(markdown, "table")
        except UserException as e:
            return ValidationResult(f"Query Failed: {e}", MessageType.WARNING)

    @sync_action("loadObjects")
    def load_possible_objects(self) -> list[SelectElement]:
        """
        Finds all possible objects in Salesforce that can be fetched by the Bulk API

        Returns: a List of dictionaries containing 'name' and 'value' of the SF object, where 'name' is the Label name/
        readable name of the object, and 'value' is the name of the object you can use to query the object

        """
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        return [SelectElement(**c) for c in salesforce_client.get_bulk_fetchable_objects()]

    @sync_action("loadFields")
    def load_fields(self) -> list[SelectElement]:
        """Returns fields available for selected object."""
        params = self.configuration.parameters
        object_name = params.get("object")
        salesforce_client = self.get_salesforce_client(params)
        descriptions = salesforce_client.describe_object_w_metadata(object_name)
        return [SelectElement(label=f"{field[0]} ({field[1]})", value=field[0]) for field in descriptions]

    @sync_action("loadPossibleIncrementalField")
    def load_possible_incremental_field(self) -> list[SelectElement]:
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
    def load_possible_primary_keys(self) -> list[SelectElement]:
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
        query = self.build_soql_query(salesforce_client, params)
        return query.sf_object

    def _get_object_fields_names_and_normalized_values(self, object_name: str) -> list[SelectElement]:
        """
        Return object fields for sync action

        Args:
            object_name:

        Returns:

        """
        columns = self._get_fields_of_object_by_name(object_name)
        column_values = self.normalize_column_names(columns)
        return [SelectElement(label=column, value=column_values[i]) for i, column in enumerate(columns)]

    def _get_object_fields_names_and_values(self, object_name: str) -> list[SelectElement]:
        columns = self._get_fields_of_object_by_name(object_name)
        return [SelectElement(label=column, value=column) for column in columns]

    def _get_fields_of_object_by_name(self, object_name: str) -> list[str]:
        params = self.configuration.parameters
        salesforce_client = self.get_salesforce_client(params)
        return salesforce_client.describe_object(object_name)

    def _get_object_fields_from_query(self) -> list[SelectElement]:
        """
        Return object fields for sync action
        Returns:

        """
        result = self._get_first_result_from_custom_soql()

        if not result:
            raise UserException(
                "Failed to determine fields from SOQL query, "
                "make sure the SOQL query is valid and that it returns data."
            )

        columns = list(result.keys())
        if "attributes" in columns:
            columns.remove("attributes")
        column_values = self.normalize_column_names(columns)

        return [SelectElement(label=column, value=column_values[i]) for i, column in enumerate(columns)]

    def _get_first_result_from_custom_soql(self) -> dict:
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
