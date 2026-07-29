'''
Created on 12. 11. 2018

@author: esner
'''
import contextlib
import json
import tempfile
import unittest
import mock
import os
import requests
from freezegun import freeze_time
from requests.exceptions import ConnectionError as RequestsConnectionError

from component import Component
from salesforce.client import SalesforceClient

# What requests raises when Salesforce closes the connection without sending a response.
DROPPED_CONNECTION = RequestsConnectionError(
    "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
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


class TestDroppedConnectionRetries(unittest.TestCase):
    """A dropped connection is retried instead of killing the job, and still fails the job when it persists."""

    @staticmethod
    def _build_client(session=None):
        simple_client = mock.MagicMock()
        simple_client.base_url = 'https://example.my.salesforce.com/services/data/v52.0/'
        simple_client.session_id = 'dummy-session-id'
        simple_client.session = session if session is not None else requests.Session()
        return SalesforceClient(simple_client=simple_client, api_version='52.0')

    @staticmethod
    @contextlib.contextmanager
    def _build_component(parameters):
        with tempfile.TemporaryDirectory() as data_dir:
            os.makedirs(os.path.join(data_dir, 'out', 'tables'))
            with open(os.path.join(data_dir, 'config.json'), 'w', encoding='utf-8') as config_file:
                json.dump({'parameters': parameters}, config_file)
            with mock.patch.dict(os.environ, {'KBC_DATADIR': data_dir}):
                yield Component()

    def test_transport_retries_are_mounted_on_the_salesforce_session(self):
        session = requests.Session()

        self._build_client(session)

        retries = session.get_adapter('https://example.my.salesforce.com/').max_retries
        self.assertEqual(3, retries.connect)
        self.assertEqual(3, retries.read)
        # HTTP responses must never be retried - every response the component already handled is passed through.
        self.assertEqual(0, retries.status)
        self.assertFalse(retries.status_forcelist)
        # Only idempotent methods are retried, so no request with a side effect is ever re-sent.
        self.assertNotIn('POST', retries.allowed_methods)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('salesforce.client.SFType')
    def test_describe_object_retries_dropped_connection_then_reraises(self, sf_type, _sleep):
        sf_type.return_value.describe.side_effect = DROPPED_CONNECTION
        client = self._build_client()

        with self.assertRaises(RequestsConnectionError):
            client.describe_object('Account')

        self.assertEqual(3, sf_type.return_value.describe.call_count)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('salesforce.client.SFType')
    def test_describe_object_succeeds_after_a_dropped_connection(self, sf_type, _sleep):
        sf_type.return_value.describe.side_effect = [
            DROPPED_CONNECTION,
            {'fields': [{'name': 'Id', 'type': 'id'}, {'name': 'Photo', 'type': 'base64'}]},
        ]
        client = self._build_client()

        self.assertEqual(['Id'], client.describe_object('Account'))
        self.assertEqual(2, sf_type.return_value.describe.call_count)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('component.SalesforceClient.from_security_token')
    def test_login_retries_dropped_connection_then_reraises(self, from_security_token, _sleep):
        from_security_token.side_effect = DROPPED_CONNECTION
        parameters = {'login_method': 'security_token', 'username': 'user',
                      '#password': 'password', '#security_token': 'token'}

        with self._build_component(parameters) as comp:
            with self.assertRaises(RequestsConnectionError):
                comp._login_to_salesforce(comp.configuration.parameters)

        self.assertEqual(3, from_security_token.call_count)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
