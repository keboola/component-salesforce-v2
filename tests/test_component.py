'''
Created on 12. 11. 2018

@author: esner
'''
import contextlib
import json
import logging
import socket
import tempfile
import threading
import unittest
import mock
import os
import requests
from freezegun import freeze_time
from requests.exceptions import ConnectionError as RequestsConnectionError

from component import Component
from salesforce.client import SalesforceClient


def dropped_connection() -> RequestsConnectionError:
    """A fresh instance of what requests raises when Salesforce closes the connection without responding.

    Built per call on purpose: one shared instance would be raised repeatedly and accumulate __traceback__
    frames across retries and across tests.
    """
    return RequestsConnectionError(
        "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
    )


def raise_dropped_connection(*_args, **_kwargs):
    """mock side_effect raising a fresh dropped_connection() on every call."""
    raise dropped_connection()


class LocalHttpServer:
    """A local TCP server used to exercise the mounted retry adapter against real socket behaviour.

    With no response configured it accepts, reads the request and closes without answering - the failure this
    PR is about. With one configured it replies with those exact bytes. Binds to port 0 and runs on a daemon
    thread with short timeouts so it cannot hang or collide with anything in CI.
    """

    def __init__(self, response: bytes = None) -> None:
        self._response = response
        self.connection_count = 0
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(('127.0.0.1', 0))
        self._socket.listen(8)
        self._socket.settimeout(0.5)
        self.url = f'http://127.0.0.1:{self._socket.getsockname()[1]}/'
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                connection, _ = self._socket.accept()
            except OSError:
                continue
            self.connection_count += 1
            with contextlib.closing(connection):
                # Generous, to match the client's own timeout=5. A recv() timing out on a loaded runner would look
                # like a dropped connection to the client and fail the 429 test as if it were a product bug. Nothing
                # waits on this in the happy path, so the headroom costs no test time. The listening socket keeps its
                # short accept timeout - the serve loop just continues on it.
                connection.settimeout(5)
                try:
                    connection.recv(65536)
                    if self._response is not None:
                        connection.sendall(self._response)
                except OSError:
                    pass

    def close(self) -> None:
        self._stop.set()
        self._socket.close()
        self._thread.join(timeout=2)


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
            # ComponentBase.__init__ adds root log handlers without removing the ones already there, so a second
            # Component() in-process leaves the root logger with a duplicate stderr handler and every retry warning
            # emitted afterwards is printed twice - which reads as a larger retry budget than the code actually has.
            # Clearing before construction (not just restoring afterwards) is what keeps the counts honest while the
            # warnings are emitted; the originals go back in the finally.
            root_logger = logging.getLogger()
            original_handlers = root_logger.handlers[:]
            root_logger.handlers.clear()
            try:
                with mock.patch.dict(os.environ, {'KBC_DATADIR': data_dir}):
                    yield Component()
            finally:
                root_logger.handlers[:] = original_handlers

    def test_transport_retries_are_mounted_on_the_salesforce_session(self):
        session = requests.Session()

        self._build_client(session)

        retries = session.get_adapter('https://example.my.salesforce.com/').max_retries
        self.assertEqual(3, retries.connect)
        self.assertEqual(3, retries.read)
        # Both socket tests below patch this factor away, so nothing else holds the documented timing: at 1 the
        # sleeps are 0, 2 and 4 s, about 6 s added to a request that ultimately fails. Raising it would silently
        # multiply that on every failing request, including the sync-action paths a user waits on.
        self.assertEqual(1, retries.backoff_factor)
        # HTTP responses must never be retried - every response the component already handled is passed through.
        # total=None is what makes Retry.is_retry() False for every status, so pin it rather than leave it
        # incidental; status/status_forcelist keep it False even if total is ever given a number.
        self.assertIsNone(retries.total)
        self.assertEqual(0, retries.status)
        self.assertFalse(retries.status_forcelist)
        self.assertFalse(retries.respect_retry_after_header)
        # allowed_methods gates urllib3's read branch only, so this proves POST is not retried *after* the request
        # went out - not that POST is never retried. Connect-phase failures, which urllib3 raises only when the
        # server provably never received the request, are retried for every method.
        self.assertNotIn('POST', retries.allowed_methods)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('salesforce.client.SFType')
    def test_describe_object_retries_dropped_connection_then_reraises(self, sf_type, _sleep):
        sf_type.return_value.describe.side_effect = raise_dropped_connection
        client = self._build_client()

        with self.assertRaises(RequestsConnectionError):
            client.describe_object('Account')

        self.assertEqual(3, sf_type.return_value.describe.call_count)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('salesforce.client.SFType')
    def test_describe_object_succeeds_after_a_dropped_connection(self, sf_type, _sleep):
        sf_type.return_value.describe.side_effect = [
            dropped_connection(),
            {'fields': [{'name': 'Id', 'type': 'id'}, {'name': 'Photo', 'type': 'base64'}]},
        ]
        client = self._build_client()

        self.assertEqual(['Id'], client.describe_object('Account'))
        self.assertEqual(2, sf_type.return_value.describe.call_count)

    @mock.patch('time.sleep', return_value=None)
    @mock.patch('component.SalesforceClient.from_security_token')
    def test_login_retries_dropped_connection_then_reraises(self, from_security_token, _sleep):
        from_security_token.side_effect = raise_dropped_connection
        parameters = {'login_method': 'security_token', 'username': 'user',
                      '#password': 'password', '#security_token': 'token'}

        with self._build_component(parameters) as comp:
            with self.assertRaises(RequestsConnectionError):
                comp._login_to_salesforce(comp.configuration.parameters)

        self.assertEqual(3, from_security_token.call_count)

    @mock.patch('salesforce.client.TRANSPORT_RETRY_BACKOFF_FACTOR', 0)
    def test_adapter_retries_a_real_dropped_connection_and_still_raises_connection_error(self):
        server = LocalHttpServer()
        self.addCleanup(server.close)
        session = requests.Session()
        SalesforceClient._mount_transport_retries(session)

        with self.assertRaises(RequestsConnectionError):
            session.get(server.url, timeout=5)

        # One initial attempt plus the three configured retries.
        self.assertEqual(4, server.connection_count)

    @mock.patch('salesforce.client.TRANSPORT_RETRY_BACKOFF_FACTOR', 0)
    def test_adapter_does_not_resend_a_post_that_already_reached_the_server(self):
        server = LocalHttpServer()
        self.addCleanup(server.close)
        session = requests.Session()
        SalesforceClient._mount_transport_retries(session)

        with self.assertRaises(RequestsConnectionError):
            session.post(server.url, data=b'{}', timeout=5)

        # The load-bearing invariant of this change, asserted behaviourally rather than through allowed_methods:
        # the server read the request and then dropped the connection, and urllib3's read branch re-raises
        # immediately for POST. So a Bulk 2.0 job-creation POST is never sent twice and cannot create a second bulk
        # job. Same server, same drop, over GET takes 4 connections - see the test above.
        self.assertEqual(1, server.connection_count)

    @mock.patch('salesforce.client.TRANSPORT_RETRY_BACKOFF_FACTOR', 0)
    def test_adapter_passes_a_429_with_retry_after_straight_through(self):
        server = LocalHttpServer(
            response=b'HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\nContent-Length: 0\r\n\r\n'
        )
        self.addCleanup(server.close)
        session = requests.Session()
        SalesforceClient._mount_transport_retries(session)

        response = session.get(server.url, timeout=5)

        self.assertEqual(429, response.status_code)
        self.assertEqual(1, server.connection_count)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
