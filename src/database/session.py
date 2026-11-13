"""Session lifecycle management for PostgreSQL database operations.

Provides sessionmaker factory configuration and a context manager that ensures
atomic transactions, automatic commits, rollbacks on error, and guaranteed cleanup.
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from src.database.connection import get_engine, get_test_engine

logger = logging.getLogger(__name__)

# Global sessionmaker factory instances
_session_factory: Optional[sessionmaker[Session]] = None
_test_session_factory: Optional[sessionmaker[Session]] = None


def get_session_factory(engine: Optional[Engine] = None) -> sessionmaker[Session]:
    """Retrieve or initialize the SQLAlchemy sessionmaker factory.

    Args:
        engine: Optional custom SQLAlchemy engine. If omitted, uses get_engine().

    Returns:
        sessionmaker[Session]: Configured sessionmaker bound to the engine.
    """
    global _session_factory
    if _session_factory is not None and engine is None:
        return _session_factory

    target_engine = engine or get_engine()
    factory = sessionmaker(
        bind=target_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )

    if engine is None:
        _session_factory = factory

    return factory


@contextmanager
def get_db_session(engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    """Context manager for managing a transactional SQLAlchemy session.

    Commits on successful block completion, rolls back on exceptions,
    and guarantees that the session is closed back to the pool.

    Args:
        engine: Optional custom SQLAlchemy engine to bind the session to.

    Yields:
        Session: Active SQLAlchemy session.

    Raises:
        SQLAlchemyError: On transaction or database operation failure.
        Exception: Any re-raised exception from the wrapped context block.
    """
    factory = get_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("Database transaction error encountered. Session rolled back: %s", exc)
        raise
    except Exception as exc:
        session.rollback()
        logger.error("Application error during database session. Session rolled back: %s", exc)
        raise
    finally:
        session.close()


def get_test_session_factory(engine: Optional[Engine] = None) -> sessionmaker[Session]:
    """Retrieve or initialize the SQLAlchemy sessionmaker factory for the TEST database."""
    global _test_session_factory
    if _test_session_factory is not None and engine is None:
        return _test_session_factory

    target_engine = engine or get_test_engine()
    factory = sessionmaker(
        bind=target_engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )

    if engine is None:
        _test_session_factory = factory

    return factory


@contextmanager
def get_test_db_session(engine: Optional[Engine] = None) -> Generator[Session, None, None]:
    """Context manager for managing a transactional SQLAlchemy session on the TEST database.

    Commits on successful block completion, rolls back on exceptions,
    and guarantees that the session is closed back to the pool.
    """
    factory = get_test_session_factory(engine)
    session: Session = factory()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as exc:
        session.rollback()
        logger.error("Test database transaction error encountered. Session rolled back: %s", exc)
        raise
    except Exception as exc:
        session.rollback()
        logger.error("Application error during test database session. Session rolled back: %s", exc)
        raise
    finally:
        session.close()


def verify_test_database(session: Session) -> str:
    """Strict safety guard: verify that the connected database is exclusively the test database.

    Returns:
        str: Name of the verified test database.

    Raises:
        RuntimeError: If connected to the development/production database or any non-test database.
    """
    current_db = session.execute(text("SELECT current_database();")).scalar()
    expected_test_db = os.getenv("TEST_DB_NAME", "cdc_test_db")
    prod_db = os.getenv("DB_NAME", "cdc_db")

    if current_db == prod_db:
        raise RuntimeError(
            f"CRITICAL SAFETY GUARD VIOLATION: Destructive operation BLOCKED! Active session is connected to "
            f"the development/production database '{current_db}' (DB_NAME). Refusing to execute test cleanup on non-test database."
        )

    if current_db != expected_test_db:
        raise RuntimeError(
            f"CRITICAL SAFETY GUARD VIOLATION: Destructive operation BLOCKED! Connected database is '{current_db}', "
            f"which does not match the configured test database '{expected_test_db}' (TEST_DB_NAME)."
        )

    return str(current_db)


def safe_test_cleanup(session: Session) -> None:
    """Safely clean all tables in the test database, guarded by strict name verification.

    Raises:
        RuntimeError: If the connected database is not the test database.
    """
    verified_db = verify_test_database(session)
    logger.info("Executing safe test cleanup on verified test database: %s", verified_db)
    session.execute(text("DELETE FROM customer_event_history;"))
    session.execute(text("DELETE FROM event_quarantine;"))
    session.execute(text("DELETE FROM customers_current;"))
    session.execute(text("DELETE FROM pipeline_runs;"))

