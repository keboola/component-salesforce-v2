'''
Template Component main class.

'''
import logging
from datetime import datetime
import unicodecsv
import csv
from os import path, mkdir

from retry import retry
from salesforce.client import SalesforceClient
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from simple_salesforce.exceptions import SalesforceResourceNotFound
from keboola.component.base import ComponentBase, UserException
from salesforce_bulk.salesforce_bulk import BulkBatchFailed

# configuration variables
KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_SANDBOX = "sandbox"
KEY_OBJECT = "object"
KEY_SOQL_QUERY = "soql_query"
KEY_INCREMENTAL = "incremental"
KEY_INCREMENTAL_FIELD = "incremental_field"
KEY_INCREMENTAL_FETCH = "incremental_fetching"
KEY_IS_DELETED = "is_deleted"
KEY_PRIVATE_KEY = "pkey"

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

        last_run = self.get_state_file().get("last_run")
        pkeys = params.get(KEY_PRIVATE_KEY)
        incremental = params.get(KEY_INCREMENTAL, False)

        try:
            salesforce_client = self.login_to_salesforce(params)
        except SalesforceAuthenticationFailed:
            raise UserException("Authentication Failed : recheck your username, password, and security token ")

        soql_query = self.build_soql_query(salesforce_client, params, last_run)

        missing_keys = soql_query.check_pkey_in_query(pkeys)
        if missing_keys != []:
            raise UserException(f"Private Keys {missing_keys} not in query, Add to SOQL query or check that it exists"
                                f" in the Salesforce object.")

        table = self.create_out_table_definition(f'{soql_query.sf_object}.csv',
                                                 primary_key=pkeys,
                                                 incremental=incremental,
                                                 is_sliced=True)

        self.create_sliced_directory(soql_query.sf_object)

        for index, (result, sf_object) in enumerate(self.fetch_result(salesforce_client, soql_query)):
            self.write_results(result, table, index)

        self.write_tabledef_manifest(table)

        soql_timestamp = str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'))
        self.write_state_file({"last_run": soql_timestamp})

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def login_to_salesforce(self, params):
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX))

    def create_sliced_directory(self, file_name):
        logging.info("Creating sliced file")
        tables_out_path = self.tables_out_path
        table_name = "".join([file_name, ".csv"])
        sliced_directory = path.join(tables_out_path, table_name)
        mkdir(sliced_directory)

    @retry(tries=3, delay=5)
    def fetch_result(self, salesforce_client, soql_query):
        result = salesforce_client.run_query(soql_query)
        sf_object = result["object"]
        try:
            for result in result["result"]:
                yield result, sf_object
        except BulkBatchFailed:
            raise UserException("Invalid Query: Failed to process query. Check syntax, objects, and fields")

    def write_results(self, result, table, index):
        slice_path = path.join(table.full_path, str(index))
        with open(slice_path, 'w+', newline='') as out:
            reader = unicodecsv.DictReader(result)
            table.columns = list(reader.fieldnames)
            writer = csv.DictWriter(out, fieldnames=reader.fieldnames, lineterminator='\n', delimiter=',')
            for row in reader:
                writer.writerow(row)

    def build_soql_query(self, salesforce_client, params, last_state):
        salesforce_object = params.get(KEY_OBJECT)
        soql_query_string = params.get(KEY_SOQL_QUERY)
        incremental = params.get(KEY_INCREMENTAL, False)
        incremental_field = params.get(KEY_INCREMENTAL_FIELD, "LastModifiedDate")
        incremental_fetching = params.get(KEY_INCREMENTAL_FETCH)
        is_deleted = params.get(KEY_IS_DELETED, False)

        try:
            if soql_query_string:
                soql_query = salesforce_client.build_query_from_string(soql_query_string)
            elif salesforce_object:
                soql_query = salesforce_client.build_soql_query_from_object_name(salesforce_object)
        except SalesforceResourceNotFound:
            raise UserException(f"Object type {salesforce_object} does not exist in Salesforce, "
                                f"enter a valid object")

        if incremental and incremental_fetching and last_state:
            soql_query.set_query_to_incremental(incremental_field, last_state)

        soql_query.set_deleted_option_in_query(is_deleted)

        return soql_query


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
