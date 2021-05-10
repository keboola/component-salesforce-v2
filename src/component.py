'''
Template Component main class.

'''
import logging
from datetime import datetime
import unicodecsv
from result import Writer
from time import sleep

from retry import retry

from salesforce_client import SalesforceClient
from salesforce_bulk.salesforce_bulk import BulkBatchFailed
from simple_salesforce.exceptions import SalesforceAuthenticationFailed

from soql_query import SoqlQuery

from keboola.component.base import ComponentBase, UserException

# configuration variables
KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_SECURITY_TOKEN = "#security_token"
KEY_SANDBOX = "sandbox"
KEY_OBJECTS = "objects"
KEY_SOQL_QUERY = "soql_query"
KEY_INCREMENTAL = "incremental"
KEY_INCREMENTAL_FIELD = "incremental_field"
KEY_IS_DELETED = "is_deleted"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_SECURITY_TOKEN]
REQUIRED_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters

        last_run = self.get_state_file().get("last_run")

        salesforce_client = self.login_to_salesforce(params)

        soql_queries = self.get_soql_queries(salesforce_client, params, last_run)
        results = self.run_queries(salesforce_client, soql_queries)

        for result in results:
            self.write_results(result, params.get(KEY_INCREMENTAL, False))

        soql_timestamp = str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z'))
        self.write_state_file({"last_run": soql_timestamp})

    @retry(SalesforceAuthenticationFailed, tries=3, delay=5)
    def login_to_salesforce(self, params):
        return SalesforceClient(username=params.get(KEY_USERNAME),
                                password=params.get(KEY_PASSWORD),
                                security_token=params.get(KEY_SECURITY_TOKEN),
                                sandbox=params.get(KEY_SANDBOX))

    def write_results(self, results, incremental):
        for result in results["result"]:
            reader = unicodecsv.DictReader(result, encoding='utf-8')
            first_row = next(reader, None)
            if first_row:
                tdf = self.create_out_table_definition(f'{results["object"]}.csv', primary_key=['Id'],
                                                       incremental=incremental)
                with Writer(tdf) as wrt:
                    tdf.columns = list(first_row.keys())
                    wrt.create_writer(tdf.columns, headerless=True)
                    wrt.write_first_row(first_row)
                    wrt.write_results_with_reader(reader)
                self.write_tabledef_manifest(tdf)

    def run_queries(self, salesforce_client, soql_queries):
        results = []
        for soql_query in soql_queries:
            results.append(self.run_query(salesforce_client, soql_query))
        return results

    @staticmethod
    @retry(tries=3, delay=5)
    def run_query(salesforce_client, soql_query):
        job = salesforce_client.create_queryall_job(soql_query.sf_object, contentType='CSV')
        batch = salesforce_client.query(job, soql_query.query)
        salesforce_client.close_job(job)
        logging.info(f"Running SOQL : {soql_query.query}")
        try:
            while not salesforce_client.is_batch_done(batch):
                sleep(10)
        except BulkBatchFailed as batch_fail:
            logging.exception(batch_fail.state_message)
        batch_result = salesforce_client.get_all_results_for_query_batch(batch)
        return {"object": soql_query.sf_object, "result": batch_result}

    def get_soql_queries(self, salesforce_client, params, last_state):
        salesforce_object = params.get(KEY_OBJECTS)
        soql_query_string = params.get(KEY_SOQL_QUERY)
        incremental = params.get(KEY_INCREMENTAL, False)
        incremental_field = params.get(KEY_INCREMENTAL_FIELD, "LastModifiedDate")
        is_deleted = params.get(KEY_IS_DELETED, False)

        if soql_query_string:
            soql_queries = self.get_soql_queries_from_queries(salesforce_client, [soql_query_string])
        else:
            soql_queries = self.get_soql_queries_from_objects(salesforce_client, salesforce_object)

        if incremental and last_state:
            for i, query in enumerate(soql_queries):
                soql_queries[i].set_query_to_incremental(incremental_field, last_state)
        if not is_deleted:
            for i, query in enumerate(soql_queries):
                soql_queries[i].remove_deleted_from_query()

        return soql_queries

    @staticmethod
    def get_soql_queries_from_queries(salesforce_client, soql_query_strings):
        soql_queries = []
        for query in soql_query_strings:
            soql_query = SoqlQuery(query=query)
            object_fields = salesforce_client.describe_object(soql_query.sf_object)
            soql_query.set_sf_object_fields(object_fields)
            soql_queries.append(soql_query)
        return soql_queries

    @staticmethod
    def get_soql_queries_from_objects(salesforce_client, sf_object):
        soql_queries = []
        sf_objects = sf_object.split(",")
        for sf_object in sf_objects:
            sf_object = sf_object.strip()
            object_fields = salesforce_client.describe_object(sf_object)
            soql_query = SoqlQuery(sf_object_fields=object_fields, sf_object=sf_object)
            soql_queries.append(soql_query)
        return soql_queries


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
