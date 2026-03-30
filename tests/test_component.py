'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from simple_salesforce.exceptions import SalesforceBulkV2LoadError, SalesforceGeneralError

from component import Component
from salesforce.client import (
    SalesforceClient,
    SalesforceClientException,
)


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestSalesforceClientRetry(unittest.TestCase):

    def _make_client(self):
        """Create a SalesforceClient with mocked simple_client."""
        simple_client = MagicMock()
        simple_client.base_url = "https://example.salesforce.com/services/data/v52.0"
        simple_client.session_id = "fake-session-id"
        simple_client.bulk2_url = "https://example.salesforce.com/services/data/v52.0/jobs/query"
        simple_client.headers = {"Authorization": "Bearer fake"}
        simple_client.session = MagicMock()
        with patch.object(SalesforceClient, '__init__', lambda self, *a, **kw: None):
            client = SalesforceClient.__new__(SalesforceClient)
            client.simple_client = simple_client
            client.api_version = "52.0"
        return client

    def _make_soql_query(self):
        """Create a mock SoqlQuery."""
        soql_query = MagicMock()
        soql_query.query = "SELECT Id FROM Account"
        soql_query.sf_object = "Account"
        return soql_query

    def _make_general_error(self, error_code):
        """Create a SalesforceGeneralError with a specific error code in the content."""
        err = SalesforceGeneralError.__new__(SalesforceGeneralError)
        err.status = 500
        err.content = f'[{{"errorCode": "{error_code}", "message": "test error"}}]'
        return err

    def test_is_retryable_error_unexpected_exception(self):
        """UNEXPECTED_EXCEPTION should be retryable."""
        err = self._make_general_error("UNEXPECTED_EXCEPTION")
        self.assertTrue(SalesforceClient._is_retryable_error(err))

    def test_is_retryable_error_server_unavailable(self):
        """SERVER_UNAVAILABLE should be retryable."""
        err = self._make_general_error("SERVER_UNAVAILABLE")
        self.assertTrue(SalesforceClient._is_retryable_error(err))

    def test_is_retryable_error_non_retryable(self):
        """A non-retryable error code should not be retryable."""
        err = self._make_general_error("INVALID_FIELD")
        self.assertFalse(SalesforceClient._is_retryable_error(err))

    def test_is_retryable_error_non_salesforce_error(self):
        """A non-Salesforce exception should not be retryable."""
        err = ValueError("some other error")
        self.assertFalse(SalesforceClient._is_retryable_error(err))

    @patch("salesforce.client.time.sleep")
    @patch("salesforce.client.SalesforceBulk2")
    def test_download_retries_on_transient_error_then_succeeds(self, mock_bulk2_cls, mock_sleep):
        """Download should retry on transient error and succeed on subsequent attempt."""
        client = self._make_client()
        soql_query = self._make_soql_query()

        transient_error = self._make_general_error("UNEXPECTED_EXCEPTION")
        mock_bulk2_instance = MagicMock()
        mock_bulk2_cls.return_value = mock_bulk2_instance
        mock_bulk2_instance.download.side_effect = [transient_error, [{"number_of_records": 10, "file": "test.csv"}]]

        results = client.download(soql_query, "/tmp/test")

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_bulk2_instance.download.call_count, 2)
        mock_sleep.assert_called_once()

    @patch("salesforce.client.time.sleep")
    @patch("salesforce.client.SalesforceBulk2")
    def test_download_raises_after_max_retries(self, mock_bulk2_cls, mock_sleep):
        """Download should raise SalesforceClientException after exhausting retries."""
        client = self._make_client()
        soql_query = self._make_soql_query()

        transient_error = self._make_general_error("UNEXPECTED_EXCEPTION")
        mock_bulk2_instance = MagicMock()
        mock_bulk2_cls.return_value = mock_bulk2_instance
        mock_bulk2_instance.download.side_effect = transient_error

        with self.assertRaises(SalesforceClientException) as ctx:
            client.download(soql_query, "/tmp/test")

        self.assertIn("Bulk download failed after", str(ctx.exception))
        self.assertEqual(mock_bulk2_instance.download.call_count, 3)

    @patch("salesforce.client.SalesforceBulk2")
    def test_download_no_retry_on_non_retryable_error(self, mock_bulk2_cls):
        """Non-retryable SalesforceGeneralError should fail immediately without retry."""
        client = self._make_client()
        soql_query = self._make_soql_query()

        non_retryable_error = self._make_general_error("INVALID_FIELD")
        mock_bulk2_instance = MagicMock()
        mock_bulk2_cls.return_value = mock_bulk2_instance
        mock_bulk2_instance.download.side_effect = non_retryable_error

        with self.assertRaises(SalesforceClientException):
            client.download(soql_query, "/tmp/test")

        self.assertEqual(mock_bulk2_instance.download.call_count, 1)

    @patch("salesforce.client.SalesforceBulk2")
    def test_download_returns_empty_list_on_bulk_load_error(self, mock_bulk2_cls):
        """SalesforceBulkV2LoadError with fail_on_error=False should return empty list."""
        client = self._make_client()
        soql_query = self._make_soql_query()

        mock_bulk2_instance = MagicMock()
        mock_bulk2_cls.return_value = mock_bulk2_instance
        mock_bulk2_instance.download.side_effect = SalesforceBulkV2LoadError("test error")

        results = client.download(soql_query, "/tmp/test", fail_on_error=False)

        self.assertEqual(results, [])

    @patch("salesforce.client.SalesforceBulk2")
    def test_download_success_no_retry(self, mock_bulk2_cls):
        """Successful download should not trigger any retries."""
        client = self._make_client()
        soql_query = self._make_soql_query()

        mock_bulk2_instance = MagicMock()
        mock_bulk2_cls.return_value = mock_bulk2_instance
        mock_bulk2_instance.download.return_value = [{"number_of_records": 5, "file": "test.csv"}]

        results = client.download(soql_query, "/tmp/test")

        self.assertEqual(len(results), 1)
        self.assertEqual(mock_bulk2_instance.download.call_count, 1)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
