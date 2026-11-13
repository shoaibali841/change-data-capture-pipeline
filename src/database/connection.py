"""Database connection management and engine configuration for the CDC pipeline.

Uses SQLAlchemy 2.0 with the PostgreSQL psycopg (v3) driver.
Loads connection parameters and pool settings securely from environment variables.
"""

import logging
import os
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy import URL, Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# Global singleton engine instances
_engine: Optional[Engine] = None
_test_engine: Optional[Engine] = None


def get_database_url() -> URL:
    """Construct the PostgreSQL connection URL safely using SQLAlchemy URL.create().

    Returns:
        URL: Safely constructed SQLAlchemy connection URL without credential string concatenation.

    Raises:
        ValueError: If any required database configuration parameter is missing.
    """
    host = os.getenv("DB_HOST", "localhost")
    port_str = os.getenv("DB_PORT", "5433")
    database = os.getenv("DB_NAME", "cdc_db")
    username = os.getenv("DB_USER", "cdc_user")
    password = os.getenv("DB_PASSWORD", "")

    if not username:
        raise ValueError("Database configuration error: DB_USER is not set.")
    if not database:
        raise ValueError("Database configuration error: DB_NAME is not set.")

    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"Invalid DB_PORT value: {port_str}. Expected an integer.") from exc

    return URL.create(
        drivername="postgresql+psycopg",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def get_engine(echo: bool = False) -> Engine:
    """Create or return the existing reusable SQLAlchemy database engine.

    Configures connection pooling from environment variables:
    - DB_POOL_SIZE: Base number of connections maintained in the pool.
    - DB_MAX_OVERFLOW: Maximum number of connections allowed beyond pool_size.
    - DB_POOL_TIMEOUT: Seconds to wait before giving up on acquiring a connection.

    Args:
        echo: If True, SQLAlchemy logs SQL statements.

    Returns:
        Engine: Initialized reusable SQLAlchemy engine.
    """
    global _engine
    if _engine is not None:
        return _engine

    url = get_database_url()
    pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
    max_overflow = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    pool_timeout = int(os.getenv("DB_POOL_TIMEOUT", "30"))

    # Log connection target with masked credentials
    masked_url = url.render_as_string(hide_password=True)
    logger.info("Initializing database engine connected to %s", masked_url)

    _engine = create_engine(
        url,
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        pool_pre_ping=True,  # Test connections for liveness before checkout
    )
    return _engine


def check_connection(engine: Optional[Engine] = None) -> bool:
    """Verify database connectivity by executing a SELECT 1 test query.

    Args:
        engine: Optional existing SQLAlchemy engine; if None, uses get_engine().

    Returns:
        bool: True if connection succeeded and returned 1, False otherwise.
    """
    eng = engine or get_engine()
    try:
        with eng.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                logger.info("Database connectivity check passed (SELECT 1 succeeded).")
                return True
            logger.warning("Database connectivity check returned unexpected result: %s", result)
            return False
    except SQLAlchemyError as exc:
        # Never log raw credentials; exc.__str__ does not reveal password when using URL.create
        logger.error("Database connection failed: %s", exc)
        return False
    except Exception as exc:
        logger.error("Unexpected error during database connectivity check: %s", exc)
        return False


def dispose_engine() -> None:
    """Dispose of the global engine and close all connections in the pool."""
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("Database engine disposed.")


def get_test_database_url() -> URL:
    """Construct the PostgreSQL connection URL safely for the TEST database.

    Returns:
        URL: Safely constructed SQLAlchemy connection URL for cdc_test_db.

    Raises:
        ValueError: If test configuration is missing or points to the production DB.
    """
    host = os.getenv("TEST_DB_HOST", os.getenv("DB_HOST", "localhost"))
    port_str = os.getenv("TEST_DB_PORT", os.getenv("DB_PORT", "5433"))
    database = os.getenv("TEST_DB_NAME", "cdc_test_db")
    username = os.getenv("TEST_DB_USER", os.getenv("DB_USER", "cdc_user"))
    password = os.getenv("TEST_DB_PASSWORD", os.getenv("DB_PASSWORD", ""))
    prod_db = os.getenv("DB_NAME", "cdc_db")

    if not database:
        raise ValueError("Test database configuration error: TEST_DB_NAME is not set.")
    if database == prod_db:
        raise ValueError(
            f"Safety guard violation: TEST_DB_NAME ('{database}') must not be the same as DB_NAME ('{prod_db}')."
        )
    if not username:
        raise ValueError("Test database configuration error: TEST_DB_USER is not set.")

    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"Invalid TEST_DB_PORT value: {port_str}. Expected an integer.") from exc

    return URL.create(
        drivername="postgresql+psycopg",
        username=username,
        password=password,
        host=host,
        port=port,
        database=database,
    )


def get_test_engine(echo: bool = False) -> Engine:
    """Create or return the existing reusable SQLAlchemy test database engine.

    Returns:
        Engine: Initialized SQLAlchemy engine bound exclusively to TEST_DB_NAME.
    """
    global _test_engine
    if _test_engine is not None:
        return _test_engine

    url = get_test_database_url()
    masked_url = url.render_as_string(hide_password=True)
    logger.info("Initializing TEST database engine connected to %s", masked_url)

    _test_engine = create_engine(
        url,
        echo=echo,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True,
    )
    return _test_engine


def dispose_test_engine() -> None:
    """Dispose of the test engine and close all connections in the pool."""
    global _test_engine
    if _test_engine is not None:
        _test_engine.dispose()
        _test_engine = None
        logger.info("Test database engine disposed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    db_url = get_database_url()
    print(f"Target DB URL: {db_url.render_as_string(hide_password=True)}")
    is_connected = check_connection()
    print(f"Connectivity Check Result: {'SUCCESS' if is_connected else 'FAILED'}")

