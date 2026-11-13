"""Pytest global configuration and database isolation fixtures.

Guarantees:
1. Integration tests run exclusively on the isolated test database (cdc_test_db).
2. Tests never silently fall back to the development database (cdc_db).
3. Any attempt to perform destructive test cleanup on cdc_db is strictly blocked.
4. Default database helpers (get_engine, get_session_factory, get_db_session, check_connection)
   are redirected to the test database during test execution.
"""

import os
from typing import Generator
import pytest

from src.database.connection import check_connection, get_test_engine
from src.database.session import (
    get_test_db_session,
    get_test_session_factory,
    safe_test_cleanup,
    verify_test_database,
)


"""Pytest global configuration and database isolation fixtures.

Guarantees:
1. Integration tests run exclusively on the isolated test database (cdc_test_db).
2. Tests never silently fall back to the development database (cdc_db).
3. Any attempt to perform destructive test cleanup on cdc_db is strictly blocked.
4. Default database helpers (get_engine, get_session_factory, get_db_session, check_connection)
   are redirected to the test database during test execution.
"""

import os
import pytest

from src.database.connection import check_connection, get_test_engine
from src.database.session import (
    get_test_db_session,
    get_test_session_factory,
    safe_test_cleanup,
    verify_test_database,
)
import src.database.connection
import src.database.session
import src.pipeline
import src.monitoring.reconciliation


def pytest_configure(config) -> None:
    """Enforce test database isolation at pytest startup by re-binding helpers to cdc_test_db."""
    test_db = os.getenv("TEST_DB_NAME", "cdc_test_db")
    prod_db = os.getenv("DB_NAME", "cdc_db")

    if test_db == prod_db:
        raise RuntimeError(
            f"CONFIGURATION ERROR: TEST_DB_NAME ('{test_db}') cannot be identical to DB_NAME ('{prod_db}'). "
            "Integration tests must run against an isolated test database."
        )

    # Verify connectivity to test database
    test_engine = get_test_engine()
    if not check_connection(engine=test_engine):
        raise ConnectionError(
            f"Cannot connect to isolated test database '{test_db}'. "
            "Ensure PostgreSQL is running and cdc_test_db exists with tables initialized."
        )

    # Double check that test engine is actually connected to test_db
    with get_test_db_session() as session:
        verified_name = verify_test_database(session)
        assert verified_name == test_db

    # Permanently bind default session and engine callers to test database during pytest
    test_factory = get_test_session_factory()
    src.database.connection.get_engine = lambda echo=False: test_engine
    src.database.session.get_engine = lambda echo=False: test_engine
    src.database.session.get_session_factory = lambda engine=None: test_factory
    src.database.session.get_db_session = get_test_db_session
    src.pipeline.get_db_session = get_test_db_session
    src.monitoring.reconciliation.get_db_session = get_test_db_session


@pytest.fixture(autouse=True)
def guard_test_database_per_test() -> None:
    """Enforce that every test transaction strictly connects to cdc_test_db."""
    with get_test_db_session() as session:
        verify_test_database(session)
