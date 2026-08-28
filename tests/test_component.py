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

from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ProtocolError

from component import Component
from salesforce.client import DOWNLOAD_RESULT_MAX_TRIES, SalesforceBulk2, SalesforceClient


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


def truncated_result_page() -> ChunkedEncodingError:
    """What requests raises out of iter_content when Salesforce cuts a result body short.

    Built the way requests builds it - urllib3 raises ProtocolError("Response ended prematurely") while reading
    the body and requests re-raises it as ChunkedEncodingError - so str() matches the message seen in production.
    """
    return ChunkedEncodingError(ProtocolError("Response ended prematurely"))


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


class TestTruncatedResultPageRetries(unittest.TestCase):
    """A result page Salesforce cut short is retried instead of killing the job, and still fails when it persists.

    The transport retries mounted on the session cannot reach this failure: it is raised while the response body
    is being read, after HTTPAdapter.send has already handed the response back. These tests pin both halves - the
    retry works, and the directory the pages are written into never gains a truncated slice because of it.
    """

    @staticmethod
    def _build_bulk2() -> SalesforceBulk2:
        simple_client = mock.MagicMock()
        simple_client.bulk2_url = 'https://example.my.salesforce.com/services/data/v52.0/jobs/'
        simple_client.headers = {}
        simple_client.session = requests.Session()
        bulk2 = SalesforceBulk2(simple_client, 'Opportunity')
        bulk2._client = mock.MagicMock()
        return bulk2

    @staticmethod
    def _page(path: str, contents: bytes, locator: str = '', number_of_records: int = 1) -> dict:
        """Writes a page file the way simple_salesforce does - a fresh temp file inside the download directory."""
        with tempfile.NamedTemporaryFile('wb', dir=path, suffix='.csv', delete=False) as page_file:
            page_file.write(contents)
        return {'locator': locator, 'number_of_records': number_of_records, 'file': page_file.name}

    @mock.patch('time.sleep', return_value=None)
    def test_truncated_page_is_retried_then_reraises(self, _sleep):
        bulk2 = self._build_bulk2()
        bulk2._client.download_job_data.side_effect = truncated_result_page()

        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ChunkedEncodingError):
                bulk2._download_result_page(path, 'job-id', '', 50000)

        self.assertEqual(DOWNLOAD_RESULT_MAX_TRIES, bulk2._client.download_job_data.call_count)

    @mock.patch('time.sleep', return_value=None)
    def test_truncated_page_succeeds_after_a_retry(self, _sleep):
        bulk2 = self._build_bulk2()

        with tempfile.TemporaryDirectory() as path:
            expected = {'locator': '', 'number_of_records': 2, 'file': 'page.csv'}
            bulk2._client.download_job_data.side_effect = [truncated_result_page(), expected]

            self.assertEqual(expected, bulk2._download_result_page(path, 'job-id', '', 50000))

        self.assertEqual(2, bulk2._client.download_job_data.call_count)
        # Every attempt asks for the same page. Re-requesting a locator is what makes the retry safe, so a change
        # that started advancing the locator between attempts - and silently skipped records - must fail here.
        self.assertEqual([mock.call(mock.ANY, 'job-id', '', 50000)] * 2,
                         bulk2._client.download_job_data.call_args_list)

    @mock.patch('time.sleep', return_value=None)
    def test_the_partial_file_of_a_retried_attempt_is_discarded(self, _sleep):
        bulk2 = self._build_bulk2()

        with tempfile.TemporaryDirectory() as path:
            already_downloaded = self._page(path, b'Id\n1\n')

            def truncate_then_succeed(page_path, *_args, **_kwargs):
                if bulk2._client.download_job_data.call_count == 1:
                    self._page(page_path, b'Id\n2')  # the half-written file simple_salesforce leaves behind
                    raise truncated_result_page()
                return self._page(page_path, b'Id\n2\n3\n')

            bulk2._client.download_job_data.side_effect = truncate_then_succeed

            result = bulk2._download_result_page(path, 'job-id', '', 50000)

            # The page downloaded before the failure survives, the truncated one is gone, and the retry's own file
            # is the only thing added - so the sliced output directory can never gain a partial or duplicate slice.
            expected = sorted([os.path.basename(already_downloaded['file']), os.path.basename(result['file'])])
            self.assertEqual(expected, sorted(os.listdir(path)))

    @mock.patch('time.sleep', return_value=None)
    def test_the_final_attempt_leaves_the_directory_exactly_as_it_used_to(self, _sleep):
        """The give-up path is untouched: same exception, and the last attempt's file is still left behind.

        This is what makes the change defensive rather than behavioural - a download that keeps failing ends in
        precisely the state it ended in before the retry existed.
        """
        bulk2 = self._build_bulk2()

        with tempfile.TemporaryDirectory() as path:
            def always_truncate(page_path, *_args, **_kwargs):
                self._page(page_path, b'Id\n2')
                raise truncated_result_page()

            bulk2._client.download_job_data.side_effect = always_truncate

            with self.assertRaises(ChunkedEncodingError):
                bulk2._download_result_page(path, 'job-id', '', 50000)

            # Only the last attempt's file remains: the two before it were cleaned up ahead of their retries.
            self.assertEqual(1, len(os.listdir(path)))

    def test_a_page_that_downloads_first_time_is_requested_once_and_never_waits(self):
        """The happy path is byte-for-byte what it was: one call, no cleanup, no sleep."""
        bulk2 = self._build_bulk2()

        with tempfile.TemporaryDirectory() as path:
            expected = self._page(path, b'Id\n1\n')
            bulk2._client.download_job_data.return_value = expected

            with mock.patch('time.sleep') as sleep:
                self.assertEqual(expected, bulk2._download_result_page(path, 'job-id', '', 50000))

            sleep.assert_not_called()
            self.assertEqual(1, bulk2._client.download_job_data.call_count)
            self.assertEqual([os.path.basename(expected['file'])], os.listdir(path))

    @mock.patch('time.sleep', return_value=None)
    def test_download_pages_through_the_retrying_helper(self, _sleep):
        """The loop that walks the locators goes through the retry, so a mid-download truncation is covered."""
        bulk2 = self._build_bulk2()

        with tempfile.TemporaryDirectory() as path:
            first = {'locator': 'page-2', 'number_of_records': 1, 'file': 'a.csv'}
            second = {'locator': '', 'number_of_records': 1, 'file': 'b.csv'}
            bulk2._client.download_job_data.side_effect = [first, truncated_result_page(), second]
            bulk2._client.create_job.return_value = {'id': 'job-id'}

            results = bulk2.download('SELECT Id FROM Opportunity', path)

        self.assertEqual([first, second], results)
        self.assertEqual(3, bulk2._client.download_job_data.call_count)

    def test_an_error_response_is_not_mistaken_for_a_truncated_body(self):
        """Anything other than a truncated body propagates on the first attempt, unretried and unchanged."""
        bulk2 = self._build_bulk2()
        bulk2._client.download_job_data.side_effect = ValueError('malformed response')

        with tempfile.TemporaryDirectory() as path:
            with self.assertRaises(ValueError):
                bulk2._download_result_page(path, 'job-id', '', 50000)

        self.assertEqual(1, bulk2._client.download_job_data.call_count)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
