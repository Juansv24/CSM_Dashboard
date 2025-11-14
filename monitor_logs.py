"""
Log Monitoring Dashboard for CSM Dashboard
Provides real-time monitoring of application errors and performance metrics
"""

import streamlit as st
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go


def load_json_logs(log_file: Path, lines: int = 1000) -> list:
    """Load JSON format logs from file"""
    logs = []
    try:
        if log_file.exists():
            with open(log_file, 'r') as f:
                # Read last N lines
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    try:
                        logs.append(json.loads(line))
                    except json.JSONDecodeError:
                        # Skip lines that aren't valid JSON
                        pass
    except Exception as e:
        st.error(f"Error reading log file {log_file}: {str(e)}")

    return logs


def load_text_logs(log_file: Path, lines: int = 1000) -> list:
    """Load text format logs from file"""
    log_entries = []
    try:
        if log_file.exists():
            with open(log_file, 'r') as f:
                all_lines = f.readlines()
                for line in all_lines[-lines:]:
                    if line.strip():
                        log_entries.append(line.strip())
    except Exception as e:
        st.error(f"Error reading log file {log_file}: {str(e)}")

    return log_entries


def analyze_error_logs() -> dict:
    """Analyze error logs and return statistics"""
    error_log = Path("logs/CSM_Dashboard_errors.log")
    log_entries = load_text_logs(error_log, lines=10000)

    stats = {
        'total_errors': len(log_entries),
        'error_types': {},
        'recent_errors': log_entries[-10:] if log_entries else []
    }

    # Categorize errors
    for entry in log_entries:
        if 'DATABASE' in entry:
            stats['error_types']['Database'] = stats['error_types'].get('Database', 0) + 1
        elif 'CONNECTION' in entry:
            stats['error_types']['Connection'] = stats['error_types'].get('Connection', 0) + 1
        elif 'TIMEOUT' in entry:
            stats['error_types']['Timeout'] = stats['error_types'].get('Timeout', 0) + 1
        elif 'METADATA' in entry:
            stats['error_types']['Metadata'] = stats['error_types'].get('Metadata', 0) + 1
        else:
            stats['error_types']['Other'] = stats['error_types'].get('Other', 0) + 1

    return stats


def analyze_performance_logs() -> dict:
    """Analyze performance logs and return metrics"""
    perf_log = Path("logs/CSM_Dashboard_performance.log")
    logs = load_json_logs(perf_log, lines=10000)

    metrics = {
        'total_operations': len(logs),
        'average_duration_ms': 0,
        'slow_operations': [],
        'operations_by_type': {}
    }

    durations = []
    for log in logs:
        if 'duration_ms' in log:
            durations.append(log['duration_ms'])

        op_type = log.get('operation', 'Unknown')
        metrics['operations_by_type'][op_type] = metrics['operations_by_type'].get(op_type, 0) + 1

        # Track slow operations (>1000ms)
        if log.get('duration_ms', 0) > 1000:
            metrics['slow_operations'].append({
                'operation': op_type,
                'duration_ms': log['duration_ms'],
                'timestamp': log.get('timestamp', 'Unknown')
            })

    if durations:
        metrics['average_duration_ms'] = sum(durations) / len(durations)
        metrics['max_duration_ms'] = max(durations)
        metrics['min_duration_ms'] = min(durations)

    return metrics


def analyze_database_logs() -> dict:
    """Analyze database logs and return statistics"""
    db_log = Path("logs/CSM_Dashboard_database.log")
    logs = load_json_logs(db_log, lines=10000)

    stats = {
        'total_db_operations': len(logs),
        'successful_operations': 0,
        'failed_operations': 0,
        'average_query_time_ms': 0,
        'operations_timeline': [],
        'operations_by_type': {}
    }

    query_times = []
    for log in logs:
        op_type = log.get('operation', 'Unknown')
        stats['operations_by_type'][op_type] = stats['operations_by_type'].get(op_type, 0) + 1

        status = log.get('status', 'Unknown')
        if status == 'SUCCESS':
            stats['successful_operations'] += 1
        elif status == 'FAILED':
            stats['failed_operations'] += 1

        if 'duration_ms' in log:
            query_times.append(log['duration_ms'])
            stats['operations_timeline'].append({
                'timestamp': log.get('timestamp', ''),
                'operation': op_type,
                'duration_ms': log['duration_ms']
            })

    if query_times:
        stats['average_query_time_ms'] = sum(query_times) / len(query_times)
        stats['max_query_time_ms'] = max(query_times)

    return stats


def analyze_session_logs() -> dict:
    """Analyze session logs and return statistics"""
    all_logs = load_json_logs(Path("logs/CSM_Dashboard_all.log"), lines=10000)

    stats = {
        'total_sessions': set(),
        'session_events': {},
        'active_sessions': 0,
        'session_timeouts': 0
    }

    for log in all_logs:
        if 'session_id' in log:
            stats['total_sessions'].add(log['session_id'])

            event = log.get('event_type', 'Unknown')
            stats['session_events'][event] = stats['session_events'].get(event, 0) + 1

            if event == 'SESSION_ACTIVE':
                stats['active_sessions'] += 1
            elif event == 'SESSION_TIMEOUT':
                stats['session_timeouts'] += 1

    stats['total_sessions'] = len(stats['total_sessions'])
    return stats


def main():
    st.set_page_config(
        page_title="CSM Dashboard - Log Monitor",
        page_icon="📊",
        layout="wide"
    )

    st.title("🔍 CSM Dashboard - Log Monitor")

    # Check if logs directory exists
    if not Path("logs").exists():
        st.error("Logs directory not found. Run the CSM Dashboard first to generate logs.")
        return

    # Tabs for different log types
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Dashboard", "Errors", "Performance", "Database", "Raw Logs"]
    )

    # ============================================================
    # TAB 1: Dashboard Overview
    # ============================================================
    with tab1:
        st.header("📊 System Overview")

        col1, col2, col3, col4 = st.columns(4)

        # Error Statistics
        error_stats = analyze_error_logs()
        with col1:
            st.metric(
                "Total Errors",
                error_stats['total_errors'],
                delta="errors detected" if error_stats['total_errors'] > 0 else "no errors"
            )

        # Session Statistics
        session_stats = analyze_session_logs()
        with col2:
            st.metric(
                "Total Sessions",
                session_stats['total_sessions'],
                delta=f"{session_stats['active_sessions']} active"
            )

        # Performance Statistics
        perf_stats = analyze_performance_logs()
        with col3:
            st.metric(
                "Avg Operation Time",
                f"{perf_stats['average_duration_ms']:.2f}ms",
                delta=f"Max: {perf_stats.get('max_duration_ms', 0):.0f}ms"
            )

        # Database Statistics
        db_stats = analyze_database_logs()
        with col4:
            success_rate = (
                db_stats['successful_operations'] / db_stats['total_db_operations'] * 100
                if db_stats['total_db_operations'] > 0 else 0
            )
            st.metric(
                "DB Success Rate",
                f"{success_rate:.1f}%",
                delta=f"{db_stats['failed_operations']} failures"
            )

        st.divider()

        # Charts
        col_charts1, col_charts2 = st.columns(2)

        with col_charts1:
            # Error types pie chart
            if error_stats['error_types']:
                fig = px.pie(
                    values=list(error_stats['error_types'].values()),
                    names=list(error_stats['error_types'].keys()),
                    title="Error Distribution by Type"
                )
                st.plotly_chart(fig, width="stretch")

        with col_charts2:
            # DB Operations pie chart
            if db_stats['operations_by_type']:
                fig = px.pie(
                    values=list(db_stats['operations_by_type'].values()),
                    names=list(db_stats['operations_by_type'].keys()),
                    title="Database Operations by Type"
                )
                st.plotly_chart(fig, width="stretch")

    # ============================================================
    # TAB 2: Error Analysis
    # ============================================================
    with tab2:
        st.header("❌ Error Logs")

        error_stats = analyze_error_logs()

        st.subheader("Error Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Errors", error_stats['total_errors'])
        with col2:
            st.metric("Error Types", len(error_stats['error_types']))

        # Error types breakdown
        if error_stats['error_types']:
            st.subheader("Errors by Type")
            error_df = pd.DataFrame(
                list(error_stats['error_types'].items()),
                columns=['Type', 'Count']
            ).sort_values('Count', ascending=False)

            fig = px.bar(error_df, x='Type', y='Count', title='Error Count by Type')
            st.plotly_chart(fig, width="stretch")

        # Recent errors
        if error_stats['recent_errors']:
            st.subheader("Recent Errors")
            st.text_area(
                "Last 10 Errors:",
                value='\n'.join(error_stats['recent_errors']),
                height=300,
                disabled=True
            )

    # ============================================================
    # TAB 3: Performance Analysis
    # ============================================================
    with tab3:
        st.header("⚡ Performance Metrics")

        perf_stats = analyze_performance_logs()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Operations", perf_stats['total_operations'])
        with col2:
            st.metric(
                "Avg Duration",
                f"{perf_stats['average_duration_ms']:.2f}ms"
            )
        with col3:
            st.metric(
                "Max Duration",
                f"{perf_stats.get('max_duration_ms', 0):.2f}ms"
            )

        # Operations by type
        if perf_stats['operations_by_type']:
            st.subheader("Operations by Type")
            ops_df = pd.DataFrame(
                list(perf_stats['operations_by_type'].items()),
                columns=['Operation', 'Count']
            ).sort_values('Count', ascending=False)

            fig = px.bar(ops_df, x='Operation', y='Count')
            st.plotly_chart(fig, width="stretch")

        # Slow operations
        if perf_stats['slow_operations']:
            st.subheader("Slow Operations (>1000ms)")
            slow_ops_df = pd.DataFrame(perf_stats['slow_operations'])
            st.dataframe(slow_ops_df, width="stretch")

    # ============================================================
    # TAB 4: Database Analysis
    # ============================================================
    with tab4:
        st.header("💾 Database Operations")

        db_stats = analyze_database_logs()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Operations", db_stats['total_db_operations'])
        with col2:
            st.metric("Successful", db_stats['successful_operations'])
        with col3:
            st.metric("Failed", db_stats['failed_operations'])
        with col4:
            success_rate = (
                db_stats['successful_operations'] / db_stats['total_db_operations'] * 100
                if db_stats['total_db_operations'] > 0 else 0
            )
            st.metric("Success Rate", f"{success_rate:.1f}%")

        # Operations timeline
        if db_stats['operations_timeline']:
            st.subheader("Operation Duration Timeline")
            timeline_df = pd.DataFrame(db_stats['operations_timeline'])

            fig = px.scatter(
                timeline_df,
                x='timestamp',
                y='duration_ms',
                color='operation',
                title='Query Duration Over Time'
            )
            st.plotly_chart(fig, width="stretch")

        # Operations by type
        if db_stats['operations_by_type']:
            st.subheader("Operations by Type")
            ops_df = pd.DataFrame(
                list(db_stats['operations_by_type'].items()),
                columns=['Operation', 'Count']
            )

            fig = px.bar(ops_df, x='Operation', y='Count')
            st.plotly_chart(fig, width="stretch")

    # ============================================================
    # TAB 5: Raw Logs
    # ============================================================
    with tab5:
        st.header("📄 Raw Logs")

        log_file = st.selectbox(
            "Select log file:",
            [
                "CSM_Dashboard_all.log",
                "CSM_Dashboard_errors.log",
                "CSM_Dashboard_performance.log",
                "CSM_Dashboard_database.log"
            ]
        )

        num_lines = st.slider("Number of lines to display:", 10, 1000, 100)

        log_path = Path("logs") / log_file

        if log_path.suffix == '.log' and any(x in log_file for x in ['performance', 'database']):
            logs = load_json_logs(log_path, lines=num_lines)
            if logs:
                st.json(logs[-num_lines:])
        else:
            logs = load_text_logs(log_path, lines=num_lines)
            if logs:
                st.text_area(
                    f"Last {num_lines} lines from {log_file}:",
                    value='\n'.join(logs[-num_lines:]),
                    height=400,
                    disabled=True
                )

    # Auto-refresh info
    st.divider()
    st.info("💡 Tip: Refresh the page to see the latest logs. Use your browser's refresh button or press F5.")


if __name__ == "__main__":
    main()
