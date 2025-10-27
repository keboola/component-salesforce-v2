import unittest
from unittest import mock
from salesforce.client import SalesforceClient
from salesforce.soql_query import SoqlQuery
from simple_salesforce.exceptions import SalesforceMalformedRequest


class TestSalesforceClient(unittest.TestCase):

    def setUp(self):
        self.mock_simple_client = mock.MagicMock()
        self.mock_simple_client.base_url = "https://test.salesforce.com"
        self.mock_simple_client.session_id = "test_session_id"
        self.client = SalesforceClient(
            simple_client=self.mock_simple_client,
            api_version="52.0"
        )

    def test_test_query_short_query_uses_get(self):
        short_query = "SELECT Id, Name FROM Account LIMIT 1"
        soql_query = mock.MagicMock()
        soql_query.query = short_query
        
        expected_result = {'records': [{'Id': '001', 'Name': 'Test'}]}
        self.mock_simple_client.query.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=False)
        
        self.mock_simple_client.query.assert_called_once_with(short_query)
        self.assertEqual(result, expected_result)

    def test_test_query_long_query_uses_post(self):
        long_query = "SELECT " + ",".join([f"Field{i}" for i in range(200)]) + " FROM Account"
        soql_query = mock.MagicMock()
        soql_query.query = long_query
        
        expected_result = {'records': [{'Id': '001'}]}
        self.mock_simple_client.restful.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=False)
        
        self.mock_simple_client.restful.assert_called_once_with(
            'query', method='POST', json={'q': long_query}
        )
        self.assertEqual(result, expected_result)

    def test_test_query_with_deleted_short_uses_query_all(self):
        short_query = "SELECT Id, Name FROM Account LIMIT 1"
        soql_query = mock.MagicMock()
        soql_query.query = short_query
        
        expected_result = {'records': [{'Id': '001', 'Name': 'Test'}]}
        self.mock_simple_client.query_all.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=True)
        
        self.mock_simple_client.query_all.assert_called_once_with(short_query)
        self.assertEqual(result, expected_result)

    def test_test_query_with_deleted_long_uses_post_query_all(self):
        long_query = "SELECT " + ",".join([f"Field{i}" for i in range(200)]) + " FROM Account"
        soql_query = mock.MagicMock()
        soql_query.query = long_query
        
        expected_result = {'records': [{'Id': '001'}]}
        self.mock_simple_client.restful.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=True)
        
        self.mock_simple_client.restful.assert_called_once_with(
            'queryAll', method='POST', json={'q': long_query}
        )
        self.assertEqual(result, expected_result)

    def test_test_query_retries_with_post_on_431_error(self):
        short_query = "SELECT Id, Name FROM Account LIMIT 1"
        soql_query = mock.MagicMock()
        soql_query.query = short_query
        
        error_431 = SalesforceMalformedRequest('http://test.com', 431, 'resource', 'Error 431')
        self.mock_simple_client.query.side_effect = error_431
        
        expected_result = {'records': [{'Id': '001'}]}
        self.mock_simple_client.restful.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=False)
        
        self.mock_simple_client.query.assert_called_once()
        self.mock_simple_client.restful.assert_called_once_with(
            'query', method='POST', json={'q': short_query}
        )
        self.assertEqual(result, expected_result)

    def test_test_query_retries_with_post_on_414_error(self):
        short_query = "SELECT Id, Name FROM Account LIMIT 1"
        soql_query = mock.MagicMock()
        soql_query.query = short_query
        
        error_414 = SalesforceMalformedRequest('http://test.com', 414, 'resource', 'Error 414')
        self.mock_simple_client.query.side_effect = error_414
        
        expected_result = {'records': [{'Id': '001'}]}
        self.mock_simple_client.restful.return_value = expected_result
        
        result = self.client.test_query(soql_query, add_limit=False, include_deleted=False)
        
        self.mock_simple_client.query.assert_called_once()
        self.mock_simple_client.restful.assert_called_once_with(
            'query', method='POST', json={'q': short_query}
        )
        self.assertEqual(result, expected_result)

    def test_test_query_add_limit(self):
        short_query = "SELECT Id, Name FROM Account"
        soql_query = mock.MagicMock()
        soql_query.query = short_query
        
        expected_result = {'records': [{'Id': '001'}]}
        self.mock_simple_client.query.return_value = expected_result
        
        with mock.patch('salesforce.client.copy.deepcopy') as mock_deepcopy:
            mock_copy = mock.MagicMock()
            mock_copy.query = short_query
            mock_deepcopy.return_value = mock_copy
            
            self.client.test_query(soql_query, add_limit=True, include_deleted=False)
            
            mock_copy.add_limit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
