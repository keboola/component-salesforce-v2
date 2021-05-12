'''
Template Component main class.

'''
import logging
from datetime import datetime
import unicodecsv
import csv

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

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_SECURITY_TOKEN, [KEY_SOQL_QUERY, KEY_OBJECT]]
REQUIRED_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters

        last_run = self.get_state_file().get("last_run")

        try:
            salesforce_client = self.login_to_salesforce(params)
        except SalesforceAuthenticationFailed:
            raise UserException("Authentication Failed : recheck your username, password, and security token ")

        soql_query = self.build_soql_query(salesforce_client, params, last_run)

        results = salesforce_client.run_query(soql_query)
        try:
            sf_object = results["object"]
            result = next(results["result"])
        except BulkBatchFailed as bulk_exception:
            raise UserException(f"Invalid Query: {bulk_exception}")

        self.write_results(result, sf_object, params.get(KEY_INCREMENTAL, False))

        soql_timestamp = str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'))
        self.write_state_file({"last_run": soql_timestamp})

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def login_to_salesforce(self, params):
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX))

    def write_results(self, result, sf_object, incremental):
        tdf = self.create_out_table_definition(f'{sf_object}.csv',
                                               primary_key=['Id'],
                                               incremental=incremental)

        with open(tdf.full_path, 'w+', newline='') as out:
            reader = unicodecsv.DictReader(result)
            tdf.columns = list(reader.fieldnames)
            writer = csv.DictWriter(out, fieldnames=reader.fieldnames, lineterminator='\n', delimiter=',')
            for row in reader:
                writer.writerow(row)
        self.write_tabledef_manifest(tdf)

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
        if not is_deleted:
            soql_query.remove_deleted_from_query()

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
