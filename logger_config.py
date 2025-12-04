"""
✅ SIMPLIFIED Logging Configuration for CSM Dashboard

Replaced 392-line over-engineered logging system with simple, maintainable setup.
For a Streamlit dashboard, this provides all necessary logging functionality.
"""

import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
import time

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Create module logger
logger = logging.getLogger("CSM_Dashboard")


# Simple logging helper functions for compatibility
def log_session_event(event: str, session_id: str = "unknown", details: Optional[Dict] = None):
    """Log session lifecycle events"""
    msg = f"Session Event: {event} (ID: {session_id})"
    if details:
        msg += f" - {details}"
    logger.info(msg)


def log_error_with_context(error: Exception, context: str, session_id: str = "unknown"):
    """Log errors with context"""
    logger.error(f"Error in {context} (Session: {session_id}): {str(error)}", exc_info=True)


def log_database_operation(operation: str, table: str, status: str,
                          duration_ms: float, row_count: int = None, error: Exception = None):
    """Log database operations"""
    msg = f"DB Operation: {operation} on {table} - {status} ({duration_ms:.2f}ms)"
    if row_count:
        msg += f" - {row_count:,} rows"
    if error:
        logger.error(f"{msg} - Error: {str(error)}")
    else:
        logger.info(msg)


def log_query_performance(query_name: str, duration_ms: float, row_count: int = 0):
    """Log query performance"""
    logger.info(f"Query: {query_name} completed in {duration_ms:.2f}ms ({row_count:,} rows)")


def log_streamlit_event(event: str, details: Optional[Dict] = None):
    """Log Streamlit-specific events"""
    msg = f"Streamlit Event: {event}"
    if details:
        msg += f" - {details}"
    logger.info(msg)


@contextmanager
def LoggingContext(operation: str, log_details: Optional[Dict[str, Any]] = None):
    """
    Simple context manager for timing operations

    Usage:
        with LoggingContext('Database Connection'):
            # do work
            pass
    """
    start_time = time.time()
    try:
        yield
        duration_ms = (time.time() - start_time) * 1000
        logger.info(f"Success: {operation} (after {duration_ms:.2f}ms)")
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(f"Failed: {operation} (after {duration_ms:.2f}ms)", exc_info=True)
        raise
