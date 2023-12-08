import string
import unittest
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import agate
import dbt.flags as flags
import trino
from dbt.clients import agate_helper
from dbt.exceptions import DbtDatabaseError, DbtRuntimeError, FailedToConnectError

from dbt.adapters.extrica import ExtricaAdapter
from dbt.adapters.extrica.column import TRINO_VARCHAR_MAX_LENGTH, ExtricaColumn
from dbt.adapters.extrica.connections import (
    HttpScheme,
    ExtricaJwtCredentials
)

from .utils import config_from_parts_or_dicts, mock_connection


class TestExtricaAdapter(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

        profile_cfg = {
            "outputs": {
                "test": {
                    #ORIGINAL CREDENTIALS REQUIRED HERE FOR ADAPTER TEST 
                    "type": "extrica",
                    "method": "jwt",
                    "threads": 0,
                    "host": "extrica host",
                    "port": 0,
                    "catalog": "",
                    "schema": "",
                    "username": "",
                    "password": "",
                    "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                    "http_scheme": "https",
                    "session_properties": {
                        "query_max_run_time": "4h",
                        "exchange_compression": True,
                    },
                }
            },
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "query-comment": "dbt",
            "config-version": 2,
        }

        self.config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        self.assertEqual(self.config.query_comment.comment, "dbt")
        self.assertEqual(self.config.query_comment.append, False)

    @property
    def adapter(self):
        self._adapter = ExtricaAdapter(self.config)
        return self._adapter

    def test_acquire_connection(self):
        connection = self.adapter.acquire_connection("dummy")
        connection.handle

        self.assertEqual(connection.state, "open")
        self.assertIsNotNone(connection.handle)

    def test_cancel_open_connections_empty(self):
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    def test_cancel_open_connections_master(self):
        key = self.adapter.connections.get_thread_identifier()
        self.adapter.connections.thread_connections[key] = mock_connection("master")
        self.assertEqual(len(list(self.adapter.cancel_open_connections())), 0)

    @patch("dbt.adapters.extrica.ExtricaAdapter.ConnectionManager.get_thread_connection")
    def test_database_exception(self, get_thread_connection):
        self._setup_mock_exception(
            get_thread_connection, trino.exceptions.ProgrammingError("Syntax error")
        )
        with self.assertRaises(DbtDatabaseError):
            self.adapter.execute("select 1")

    @patch("dbt.adapters.extrica.ExtricaAdapter.ConnectionManager.get_thread_connection")
    def test_failed_to_connect_exception(self, get_thread_connection):
        self._setup_mock_exception(
            get_thread_connection,
            trino.exceptions.OperationalError("Failed to establish a new connection"),
        )
        with self.assertRaises(FailedToConnectError):
            self.adapter.execute("select 1")

    @patch("dbt.adapters.extrica.ExtricaAdapter.ConnectionManager.get_thread_connection")
    def test_dbt_exception(self, get_thread_connection):
        self._setup_mock_exception(get_thread_connection, Exception("Unexpected error"))
        with self.assertRaises(DbtRuntimeError):
            self.adapter.execute("select 1")

    def _setup_mock_exception(self, get_thread_connection, exception):
        connection = mock_connection("master")
        connection.handle = MagicMock()
        cursor = MagicMock()
        cursor.execute = Mock(side_effect=exception)
        connection.handle.cursor = MagicMock(return_value=cursor)
        get_thread_connection.return_value = connection


class TestTrinoAdapterAuthenticationMethods(unittest.TestCase):
    def setUp(self):
        flags.STRICT_MODE = True

    def acquire_connection_with_profile(self, profile):
        profile_cfg = {
            "outputs": {"test": profile},
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)

        return ExtricaAdapter(config).acquire_connection("dummy")

    def assert_default_connection_credentials(self, credentials):
        self.assertEqual(credentials.type, "extrica")
        self.assertEqual(credentials.database, "extricadb")
        self.assertEqual(credentials.host, "database")
        self.assertEqual(credentials.port, 5439)
        self.assertEqual(credentials.schema, "dbt_test_schema")
        self.assertEqual(credentials.http_headers, {"X-Trino-Client-Info": "dbt-trino"})
        self.assertEqual(
            credentials.session_properties,
            {"query_max_run_time": "4h", "exchange_compression": True},
        )
        self.assertEqual(credentials.prepared_statements_enabled, True)
        self.assertEqual(credentials.retries, trino.constants.DEFAULT_MAX_ATTEMPTS)

    def test_jwt_authentication(self):
        connection = self.acquire_connection_with_profile(
            {
                    "type": "extrica",
                    "method": "jwt",
                    "threads": 4,
                    "host": "database",
                    "port": 5439,
                    "catalog": "extricadb",
                    "schema": "dbt_test_schema",
                    "username": "test_user",
                    "password": "test_password",
                    "jwt_token":"some_token",
                    "http_headers": {"X-Trino-Client-Info": "dbt-trino"},
                    "http_scheme": "https",
                    "session_properties": {
                        "query_max_run_time": "4h",
                        "exchange_compression": True,
                    },
            }
        )
        credentials = connection.credentials
        self.assertIsInstance(credentials, ExtricaJwtCredentials)
        self.assert_default_connection_credentials(credentials)

class TestPreparedStatementsEnabled(TestCase):
    def setup_profile(self, credentials):
        profile_cfg = {
            "outputs": {"test": credentials},
            "target": "test",
        }

        project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": True,
            },
            "config-version": 2,
        }

        config = config_from_parts_or_dicts(project_cfg, profile_cfg)
        adapter = ExtricaAdapter(config)
        connection = adapter.acquire_connection("dummy")
        return connection

    def test_default(self):
        connection = self.setup_profile(
            {
                "type": "extrica",
                "catalog": "extricadb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "jwt",
                "username": "test_user",
                "password": "test_password",
                "http_scheme": "https"
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)

    def test_false(self):
        connection = self.setup_profile(
            {
                "type": "extrica",
                "catalog": "extricadb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "jwt",
                "http_scheme": "https",
                "username": "test_user",
                "password": "test_password",
                "prepared_statements_enabled": False,
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, False)

    def test_true(self):
        connection = self.setup_profile(
            {
                "type": "extrica",
                "catalog": "extricadb",
                "host": "database",
                "port": 5439,
                "schema": "dbt_test_schema",
                "method": "jwt",
                "http_scheme": "https",
                "username": "test_user",
                "password": "test_password",      
                "prepared_statements_enabled": True,
            }
        )
        self.assertEqual(connection.credentials.prepared_statements_enabled, True)


class TestAdapterConversions(TestCase):
    def _get_tester_for(self, column_type):
        from dbt.clients import agate_helper

        if column_type is agate.TimeDelta:  # dbt never makes this!
            return agate.TimeDelta()

        for instance in agate_helper.DEFAULT_TYPE_TESTER._possible_types:
            if isinstance(instance, column_type):
                return instance

        raise ValueError(f"no tester for {column_type}")

    def _make_table_of(self, rows, column_types):
        column_names = list(string.ascii_letters[: len(rows[0])])
        if isinstance(column_types, type):
            column_types = [self._get_tester_for(column_types) for _ in column_names]
        else:
            column_types = [self._get_tester_for(typ) for typ in column_types]
        table = agate.Table(rows, column_names=column_names, column_types=column_types)
        return table


class TestExtricaAdapterConversions(TestAdapterConversions):
    def test_convert_text_type(self):
        rows = [
            ["", "a1", "stringval1"],
            ["", "a2", "stringvalasdfasdfasdfa"],
            ["", "a3", "stringval3"],
        ]
        agate_table = self._make_table_of(rows, agate.Text)
        expected = ["VARCHAR", "VARCHAR", "VARCHAR"]
        for col_idx, expect in enumerate(expected):
            assert ExtricaAdapter.convert_text_type(agate_table, col_idx) == expect

    def test_convert_number_type(self):
        rows = [
            ["", "23.98", "-1"],
            ["", "12.78", "-2"],
            ["", "79.41", "-3"],
        ]
        agate_table = self._make_table_of(rows, agate.Number)
        expected = ["INTEGER", "DOUBLE", "INTEGER"]
        for col_idx, expect in enumerate(expected):
            assert ExtricaAdapter.convert_number_type(agate_table, col_idx) == expect

    def test_convert_boolean_type(self):
        rows = [
            ["", "false", "true"],
            ["", "false", "false"],
            ["", "false", "true"],
        ]
        agate_table = self._make_table_of(rows, agate.Boolean)
        expected = ["boolean", "boolean", "boolean"]
        for col_idx, expect in enumerate(expected):
            assert ExtricaAdapter.convert_boolean_type(agate_table, col_idx) == expect

    def test_convert_datetime_type(self):
        rows = [
            ["", "20190101T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190102T01:01:01Z", "2019-01-01 01:01:01"],
            ["", "20190103T01:01:01Z", "2019-01-01 01:01:01"],
        ]
        agate_table = self._make_table_of(
            rows, [agate.DateTime, agate_helper.ISODateTime, agate.DateTime]
        )
        expected = ["TIMESTAMP", "TIMESTAMP", "TIMESTAMP"]
        for col_idx, expect in enumerate(expected):
            assert ExtricaAdapter.convert_datetime_type(agate_table, col_idx) == expect

    def test_convert_date_type(self):
        rows = [
            ["", "2019-01-01", "2019-01-04"],
            ["", "2019-01-02", "2019-01-04"],
            ["", "2019-01-03", "2019-01-04"],
        ]
        agate_table = self._make_table_of(rows, agate.Date)
        expected = ["DATE", "DATE", "DATE"]
        for col_idx, expect in enumerate(expected):
            assert ExtricaAdapter.convert_date_type(agate_table, col_idx) == expect


class TestTrinoColumn(unittest.TestCase):
    def test_bound_varchar(self):
        col = ExtricaColumn.from_description("my_col", "VARCHAR(100)")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size == 100
        # bounded varchars get formatted to lowercase
        assert col.data_type == "varchar(100)"
        assert col.string_size() == 100
        assert col.is_string() is True
        assert col.is_number() is False
        assert col.is_numeric() is False

    def test_unbound_varchar(self):
        col = ExtricaColumn.from_description("my_col", "VARCHAR")
        assert col.column == "my_col"
        assert col.dtype == "VARCHAR"
        assert col.char_size is None
        assert col.data_type == "VARCHAR"
        assert col.string_size() == TRINO_VARCHAR_MAX_LENGTH
        assert col.is_string() is True
        assert col.is_number() is False
        assert col.is_numeric() is False
