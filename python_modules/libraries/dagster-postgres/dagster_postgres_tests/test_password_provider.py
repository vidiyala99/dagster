import pytest
import sqlalchemy
from dagster_postgres.utils import get_conn_string, setup_pg_password_provider_event
from dagster._core.errors import DagsterInvariantViolationError
from dagster import _check as check


def dummy_valid_provider():
    return "secret123"

def test_password_provider_valid_hook():
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    # We use this test module itself as the provider module
    setup_pg_password_provider_event(engine, "dagster_postgres_tests.test_password_provider.dummy_valid_provider")

    with pytest.raises(TypeError, match="password"):
        with engine.connect() as conn:
            pass

def test_password_provider_invalid_format():
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    with pytest.raises(DagsterInvariantViolationError, match="password_provider must be a dot-separated string"):
        setup_pg_password_provider_event(engine, "invalid_format_no_dots")

def test_password_provider_missing_attribute():
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    # Pass a valid module path but point to a non-existent variable
    with pytest.raises(DagsterInvariantViolationError, match="Could not find callable"):
        setup_pg_password_provider_event(engine, "dagster.NON_EXISTENT_ATTRIBUTE_DOES_NOT_EXIST")

NON_CALLABLE_VAR = "I am a string, not a function"

def test_password_provider_fails_runtime_callable_check():
    engine = sqlalchemy.create_engine("sqlite:///:memory:")
    
    with pytest.raises(check.CheckError, match="not callable"):
        setup_pg_password_provider_event(engine, "dagster_postgres_tests.test_password_provider.NON_CALLABLE_VAR")

def test_get_conn_string_validation():
    # Only password is valid
    uri = get_conn_string(username="foo", password="bar", hostname="host", db_name="db")
    assert uri == "postgresql://foo:bar@host:5432/db"

    # Only password_provider is valid
    uri = get_conn_string(username="foo", password_provider="my_module.get_token", hostname="host", db_name="db")
    assert uri == "postgresql://foo:@host:5432/db"

    # Fails if both are missing
    with pytest.raises(check.CheckError, match="postgres storage config must provide exactly one of `password` or `password_provider`"):
        get_conn_string(username="foo", hostname="host", db_name="db")

    # Fails if both are provided
    with pytest.raises(check.CheckError, match="postgres storage config must provide exactly one of `password` or `password_provider`"):
        get_conn_string(username="foo", password="bar", hostname="host", db_name="db", password_provider="my_module.get_token")
