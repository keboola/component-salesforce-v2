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

from component import DEFAULT_LOGIN_METHOD, Component, LoginType
from keboola.component.exceptions import UserException
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


class TestLoginMethodRetirement(unittest.TestCase):
    """The two username-and-password login methods warn, and no stored configuration changes behaviour."""

    _CREDENTIALS = {
        'username': 'user@example.com',
        '#password': 'password',
        '#security_token': 'token',
        '#consumer_key': 'key',
        '#consumer_secret': 'secret',
        'domain': 'example.my.salesforce.com',
        'sandbox': False,
        'api_version': '52.0',
    }

    @contextlib.contextmanager
    def _component(self, parameters):
        # Same handler juggling as TestDroppedConnectionRetries._build_component: ComponentBase.__init__ appends
        # root handlers without removing the existing ones, so without this a second Component() in-process makes
        # assertLogs see every record twice.
        with tempfile.TemporaryDirectory() as data_dir:
            with open(os.path.join(data_dir, 'config.json'), 'w', encoding='utf-8') as config_file:
                json.dump({'parameters': parameters}, config_file)
            root_logger = logging.getLogger()
            original_handlers = root_logger.handlers[:]
            root_logger.handlers.clear()
            try:
                with mock.patch.dict(os.environ, {'KBC_DATADIR': data_dir}):
                    yield Component()
            finally:
                root_logger.handlers[:] = original_handlers

    def _resolve(self, parameters):
        """Resolve the login method and return (resolved method, captured log records)."""
        with self._component(parameters) as component:
            with self.assertLogs(level=logging.INFO) as captured:
                resolved = component._get_login_method()
        return resolved, captured.records

    def test_configuration_without_login_method_still_uses_security_token(self):
        """The backwards-compatibility guarantee of this change.

        Configurations that predate the login_method parameter hold only a username, a password and a security
        token. The run-time fallback must keep resolving to security_token for them: the schema default now
        points at connected_app_oauth_cc, and if that leaked into the fallback every one of these configurations
        would fail on its next run with "Parameter 'domain' is needed".
        """
        parameters = {key: value for key, value in self._CREDENTIALS.items()
                      if key in ('username', '#password', '#security_token', 'sandbox', 'api_version')}

        resolved, records = self._resolve(parameters)

        self.assertEqual(LoginType.SECURITY_TOKEN_LOGIN, resolved)
        info = [record for record in records if record.levelno == logging.INFO]
        self.assertTrue(any('falling back to the default' in record.getMessage() for record in info), info)

    def test_security_token_warns_about_the_soap_login_retirement(self):
        resolved, records = self._resolve({**self._CREDENTIALS, 'login_method': 'security_token'})

        self.assertEqual(LoginType.SECURITY_TOKEN_LOGIN, resolved)
        warnings = [record.getMessage() for record in records if record.levelno == logging.WARNING]
        self.assertEqual(1, len(warnings), warnings)
        self.assertIn('connected_app_oauth_cc', warnings[0])
        self.assertIn("Summer '27", warnings[0])

    def test_connected_app_warns_about_the_username_password_flow_retirement(self):
        resolved, records = self._resolve({**self._CREDENTIALS, 'login_method': 'connected_app'})

        self.assertEqual(LoginType.CONNECTED_APP_LOGIN, resolved)
        warnings = [record.getMessage() for record in records if record.levelno == logging.WARNING]
        self.assertEqual(1, len(warnings), warnings)
        self.assertIn('connected_app_oauth_cc', warnings[0])
        self.assertIn('2027-02-20', warnings[0])

    def test_client_credentials_flow_is_not_deprecated_and_does_not_warn(self):
        resolved, records = self._resolve({**self._CREDENTIALS, 'login_method': 'connected_app_oauth_cc'})

        self.assertEqual(LoginType.CONNECTED_APP_OAUTH_CC, resolved)
        self.assertEqual([], [record.getMessage() for record in records if record.levelno >= logging.WARNING])

    def test_the_warning_is_reported_once_even_though_the_login_is_retried(self):
        """_login_to_salesforce carries @retry(tries=3), so it resolves the login method up to three times."""
        with self._component({**self._CREDENTIALS, 'login_method': 'security_token'}) as component:
            with self.assertLogs(level=logging.INFO) as captured:
                for _ in range(3):
                    component._get_login_method()

        warnings = [record for record in captured.records if record.levelno == logging.WARNING]
        self.assertEqual(1, len(warnings))

    def test_an_unknown_login_method_still_fails_with_the_same_user_error(self):
        with self._component({**self._CREDENTIALS, 'login_method': 'nonsense'}) as component:
            with self.assertRaises(UserException) as raised:
                component._get_login_method()
        self.assertIn('is not a valid Login Type', str(raised.exception))


class TestConfigSchemaLoginMethod(unittest.TestCase):
    """The schema default must not be flipped to the surviving method before stored configurations are migrated."""

    @staticmethod
    def _schema():
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'component_config', 'configSchema.json')
        with open(schema_path, encoding='utf-8') as schema_file:
            return json.load(schema_file)

    def test_the_schema_default_matches_the_run_time_fallback(self):
        """The invariant that keeps legacy configurations working. Do not relax it without a config migration.

        The two defaults look independent but are not. The Keboola config UI fills a property that a stored
        configuration does not have with the schema default and writes it back the first time a user opens and
        saves the configuration, for any edit. A configuration created before login_method existed holds a
        username, a password and a security token and no login_method. If the schema default pointed at
        connected_app_oauth_cc, an unrelated edit would write that method into it and the next run would fail
        with "Parameter 'domain' is needed" - the exact group DEFAULT_LOGIN_METHOD exists to protect.

        Making connected_app_oauth_cc the default is therefore not a change to this file alone. Backfill
        "login_method": "security_token" into every stored configuration that lacks it first; after that no
        configuration depends on either default and both can be moved together.
        """
        self.assertEqual(DEFAULT_LOGIN_METHOD, self._schema()['properties']['login_method']['default'])

    def test_no_field_hidden_by_the_default_login_method_is_required(self):
        """A field hidden by options.dependencies cannot be filled in, so requiring it makes the form unsaveable.

        This is the second trap in moving the default: username and #password are in the root required list and
        are shown only for the two username and password methods.
        """
        schema = self._schema()
        default_method = schema['properties']['login_method']['default']

        for field in schema.get('required', []):
            dependencies = schema['properties'][field].get('options', {}).get('dependencies', {})
            allowed = dependencies.get('login_method')
            if allowed is None:
                continue
            allowed = [allowed] if isinstance(allowed, str) else allowed
            self.assertIn(default_method, allowed, f"required field '{field}' is hidden for the default method")

    def test_every_login_method_in_the_code_is_offered_by_the_schema(self):
        login_method = self._schema()['properties']['login_method']

        self.assertEqual(LoginType.list(), login_method['enum'])
        self.assertEqual(len(login_method['enum']), len(login_method['options']['enum_titles']))

    def test_both_retiring_methods_are_marked_deprecated_in_the_dropdown(self):
        login_method = self._schema()['properties']['login_method']
        titles = dict(zip(login_method['enum'], login_method['options']['enum_titles']))

        self.assertIn('DEPRECATED', titles['security_token'])
        self.assertIn('DEPRECATED', titles['connected_app'])
        self.assertNotIn('DEPRECATED', titles['connected_app_oauth_cc'])
