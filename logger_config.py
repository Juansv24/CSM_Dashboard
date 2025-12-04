"""
Logging Configuration for CSM Dashboard
Provides structured logging for error monitoring, debugging, and auditing
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
import json
import traceback
from typing import Dict, Any, Optional
import streamlit as st


class JSONFormatter(logging.Formatter):
    """Custom formatter that outputs logs as JSON for easier parsing"""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'function': record.funcName,
            'line': record.lineno,
            'message': record.getMessage(),
            'module': record.module,
        }

        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }

        # Add custom attributes
        if hasattr(record, 'user_id'):
            log_data['user_id'] = record.user_id
        if hasattr(record, 'session_id'):
            log_data['session_id'] = record.session_id
        if hasattr(record, 'duration_ms'):
            log_data['duration_ms'] = record.duration_ms
        if hasattr(record, 'query_type'):
            log_data['query_type'] = record.query_type

        return json.dumps(log_data, default=str)


class StandardFormatter(logging.Formatter):
    """Standard text formatter for console output"""

    def format(self, record: logging.LogRecord) -> str:
        # Color codes for console output
        colors = {
            'DEBUG': '\033[36m',      # Cyan
            'INFO': '\033[32m',       # Green
            'WARNING': '\033[33m',    # Yellow
            'ERROR': '\033[31m',      # Red
            'CRITICAL': '\033[35m',   # Magenta
            'RESET': '\033[0m'        # Reset
        }

        color = colors.get(record.levelname, colors['RESET'])
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Build log message
        log_msg = (
            f"{color}[{timestamp}]{colors['RESET']} "
            f"{color}[{record.levelname:8}]{colors['RESET']} "
            f"{record.name}:{record.funcName}:{record.lineno} - "
            f"{record.getMessage()}"
        )

        # Add exception if present
        if record.exc_info:
            log_msg += f"\n{self.formatException(record.exc_info)}"

        return log_msg


def setup_logging(
    app_name: str = "CSM_Dashboard",
    log_dir: str = "logs",
    console_level: str = "INFO",
    file_level: str = "DEBUG"
) -> logging.Logger:
    """
    Setup comprehensive logging for the application

    Args:
        app_name: Name of the application
        log_dir: Directory to store log files
        console_level: Logging level for console output
        file_level: Logging level for file output

    Returns:
        Configured logger instance
    """

    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Create main logger
    logger = logging.getLogger(app_name)
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture everything

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create formatters
    json_formatter = JSONFormatter()
    standard_formatter = StandardFormatter()

    # ============================================================
    # Handler 1: Console Handler (Standard Formatter)
    # ============================================================
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, console_level.upper()))
    console_handler.setFormatter(standard_formatter)
    logger.addHandler(console_handler)

    # ============================================================
    # Handler 2: File Handler - All Logs (JSON Format)
    # ============================================================
    all_logs_path = log_path / f"{app_name}_all.log"
    file_handler = logging.handlers.RotatingFileHandler(
        all_logs_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5  # Keep 5 backup files
    )
    file_handler.setLevel(getattr(logging, file_level.upper()))
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)

    # ============================================================
    # Handler 3: Error Log Handler (Errors Only, Standard Format)
    # ============================================================
    error_log_path = log_path / f"{app_name}_errors.log"
    error_handler = logging.handlers.RotatingFileHandler(
        error_log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(standard_formatter)
    logger.addHandler(error_handler)

    # ============================================================
    # Handler 4: Performance Log Handler (Timing Info)
    # ============================================================
    perf_log_path = log_path / f"{app_name}_performance.log"
    perf_handler = logging.handlers.RotatingFileHandler(
        perf_log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=3
    )
    perf_handler.setLevel(logging.DEBUG)
    perf_handler.setFormatter(json_formatter)
    logger.addHandler(perf_handler)

    # ============================================================
    # Handler 5: Database Handler (DB Operations)
    # ============================================================
    db_log_path = log_path / f"{app_name}_database.log"
    db_handler = logging.handlers.RotatingFileHandler(
        db_log_path,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    db_handler.setLevel(logging.DEBUG)
    db_handler.setFormatter(json_formatter)
    logger.addHandler(db_handler)

    # Initial log
    logger.info(f"Logger initialized for {app_name}")
    logger.debug(f"Log directory: {log_path.absolute()}")

    return logger


# ============================================================
# Create global logger instance
# ============================================================
logger = setup_logging(
    app_name="CSM_Dashboard",
    log_dir="logs",
    console_level="INFO",
    file_level="DEBUG"
)


# ============================================================
# Convenience Functions for Common Logging Patterns
# ============================================================

def log_database_operation(
    operation: str,
    table: str,
    status: str,
    duration_ms: float = 0,
    row_count: int = 0,
    error: Optional[Exception] = None
) -> None:
    """Log database operations with structured data"""
    log_data = {
        'operation': operation,
        'table': table,
        'status': status,
        'duration_ms': duration_ms,
        'row_count': row_count,
    }

    if error:
        logger.error(
            f"DB Operation failed: {operation} on {table}",
            extra=log_data,
            exc_info=error
        )
    else:
        logger.info(
            f"DB Operation: {operation} on {table} - {row_count} rows in {duration_ms:.2f}ms",
            extra=log_data
        )


def log_query_performance(
    query_type: str,
    duration_ms: float,
    row_count: int = 0,
    query_hash: Optional[str] = None
) -> None:
    """Log query performance metrics"""
    logger.debug(
        f"Query Performance: {query_type} - {duration_ms:.2f}ms ({row_count} rows)",
        extra={
            'query_type': query_type,
            'duration_ms': duration_ms,
            'row_count': row_count,
            'query_hash': query_hash
        }
    )


def log_session_event(
    event_type: str,
    session_id: str,
    user_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """Log session-related events"""
    message = f"Session Event: {event_type}"
    extra = {
        'session_id': session_id,
        'user_id': user_id or 'unknown',
        'event_type': event_type
    }
    if details:
        extra.update(details)

    logger.info(message, extra=extra)


def log_error_with_context(
    error: Exception,
    context: str,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None
) -> None:
    """Log errors with full context"""
    extra = {
        'context': context,
        'user_id': user_id or 'unknown',
        'session_id': session_id or 'unknown',
    }

    logger.error(
        f"Error in {context}: {str(error)}",
        extra=extra,
        exc_info=error
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with the given name"""
    return logging.getLogger(name)


# ============================================================
# Performance Monitoring Context Manager
# ============================================================

class LoggingContext:
    """Context manager for timing and logging operations"""

    def __init__(
        self,
        operation_name: str,
        log_level: str = "DEBUG",
        log_details: Optional[Dict[str, Any]] = None
    ):
        self.operation_name = operation_name
        self.log_level = getattr(logging, log_level.upper())
        self.log_details = log_details or {}
        self.start_time = None
        self.duration_ms = 0

    def __enter__(self):
        import time
        self.start_time = time.time()
        logger.log(
            self.log_level,
            f"Starting: {self.operation_name}",
            extra={'operation': self.operation_name, **self.log_details}
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import time
        self.duration_ms = (time.time() - self.start_time) * 1000

        if exc_type:
            logger.error(
                f"Failed: {self.operation_name} (after {self.duration_ms:.2f}ms)",
                extra={
                    'operation': self.operation_name,
                    'duration_ms': self.duration_ms,
                    'error': str(exc_val),
                    **self.log_details
                },
                exc_info=(exc_type, exc_val, exc_tb)
            )
            return False
        else:
            logger.log(
                self.log_level,
                f"Completed: {self.operation_name} ({self.duration_ms:.2f}ms)",
                extra={
                    'operation': self.operation_name,
                    'duration_ms': self.duration_ms,
                    **self.log_details
                }
            )
            return True


# ============================================================
# Streamlit-Specific Logging
# ============================================================

def get_streamlit_session_info() -> Dict[str, Any]:
    """Extract Streamlit session information for logging"""
    try:
        if 'session_id' in st.session_state:
            return {
                'session_id': st.session_state.session_id,
                'session_start': str(st.session_state.session_start) if 'session_start' in st.session_state else 'unknown'
            }
    except:
        pass

    return {'session_id': 'unknown'}


def log_streamlit_event(
    event_name: str,
    details: Optional[Dict[str, Any]] = None
) -> None:
    """Log Streamlit-specific events with session context"""
    session_info = get_streamlit_session_info()
    extra = {**session_info, 'event': event_name}
    if details:
        extra.update(details)

    logger.info(f"Streamlit Event: {event_name}", extra=extra)


if __name__ == "__main__":
    # Test the logging setup
    print("Testing logging configuration...\n")

    logger.debug("This is a DEBUG message")
    logger.info("This is an INFO message")
    logger.warning("This is a WARNING message")
    logger.error("This is an ERROR message", exc_info=Exception("Test error"))

    print("\nLogging test complete. Check the 'logs' directory for output files.")
