"""
Shared UI Components and Helpers for Detail Pages

This module provides reusable UI components and formatting functions
to ensure consistency across all entity detail pages.
"""

import streamlit as st
from datetime import datetime
from typing import Dict, List, Optional, Any


def safe_text(value: Any, fallback: str = "—") -> str:
    """Return a display-safe string for UI labels."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError, NameError):
        pass
    if str(value).strip() == "":
        return fallback
    return str(value)


def safe_val(value: Any, fallback: float = 0) -> float:
    """Return a numeric-safe value."""
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError, NameError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def fmt_number(value: Any, fallback: str = "—") -> str:
    """Format a number with commas for display."""
    try:
        num = float(safe_val(value, None))
        if num is None:
            return fallback
        return f"{int(num):,}" if num == int(num) else f"{num:,.2f}"
    except (TypeError, ValueError):
        return fallback


def render_status_badge(status: str) -> str:
    """Return status with appropriate emoji."""
    status_emojis = {
        "OPEN": "📂",
        "ACKNOWLEDGED": "👀",
        "IN_PROGRESS": "🔄",
        "ESCALATED": "🚨",
        "RESOLVED": "✅",
        "CLOSED": "🔒",
        "RECEIVED": "📨",
        "VALIDATED": "✅",
        "PROCESSED": "⚙️",
        "FAILED": "❌",
        "REJECTED": "🚫",
        "RUNNING": "🔄",
        "SUCCESS": "✅",
        "PARTIAL_SUCCESS": "⚠️",
        "MET": "✅",
        "AT_RISK": "⚠️",
        "BREACHED": "⛔",
    }
    emoji = status_emojis.get(str(status).upper(), "❓")
    return f"{emoji} {safe_text(status)}"


def render_priority_badge(priority: str) -> str:
    """Return priority with appropriate emoji."""
    priority_emojis = {
        "CRITICAL": "🔴",
        "HIGH": "🟠",
        "MEDIUM": "🟡",
        "LOW": "🔵",
    }
    emoji = priority_emojis.get(str(priority).upper(), "⚪")
    return f"{emoji} {safe_text(priority)}"


def render_severity_badge(severity: str) -> str:
    """Return severity with appropriate emoji."""
    severity_emojis = {
        "CRITICAL": "🔴",
        "HIGH": "🟠",
        "MEDIUM": "🟡",
        "LOW": "🔵",
    }
    emoji = severity_emojis.get(str(severity).upper(), "⚪")
    return f"{emoji} {safe_text(severity)}"


def render_sla_risk_badge(sla_status: str) -> str:
    """Return SLA status with appropriate emoji."""
    sla_emojis = {
        "MET": "✅",
        "AT_RISK": "⚠️",
        "BREACHED": "⛔",
        "OPEN": "📂",
    }
    emoji = sla_emojis.get(str(sla_status).upper(), "❓")
    return f"{emoji} {safe_text(sla_status)}"


def render_entity_header(
    title: str,
    subtitle: str = None,
    status: str = None,
    priority: str = None,
    assignee: str = None,
    updated_at: str = None
) -> None:
    """Render a standardized entity header."""
    # Title and subtitle
    st.subheader(title)
    if subtitle:
        st.caption(subtitle)

    # Status badges and metadata
    cols = st.columns([2, 1, 1] if assignee else [2, 1])
    with cols[0]:
        if status:
            st.markdown(f"**Status:** {render_status_badge(status)}")
        if priority:
            st.markdown(f"**Priority:** {render_priority_badge(priority)}")
        if updated_at:
            st.markdown(f"**Updated:** {safe_text(updated_at)}")
    if assignee:
        with cols[1]:
            st.metric("Assignee", safe_text(assignee, "Unassigned"))
    if len(cols) > 2:
        with cols[2]:
            st.metric("Last Updated", safe_text(updated_at))


def render_metric_row(metrics: Dict[str, Any]) -> None:
    """Render a standardized metrics row."""
    if not metrics:
        return

    cols = st.columns(len(metrics))
    for i, (label, value) in enumerate(metrics.items()):
        with cols[i]:
            if isinstance(value, (int, float)):
                st.metric(label, fmt_number(value))
            else:
                st.metric(label, safe_text(value))


def render_context_section(
    title: str,
    items: Dict[str, Any],
    columns: int = 2
) -> None:
    """Render a context/metadata section with key-value pairs."""
    st.markdown(f"### {title}")
    if not items:
        st.write("No information available.")
        return

    cols = st.columns(columns)
    col_idx = 0
    for key, value in items.items():
        with cols[col_idx % columns]:
            if isinstance(value, (int, float)):
                st.markdown(f"**{key}:** {fmt_number(value)}")
            else:
                st.markdown(f"**{key}:** {safe_text(value)}")
        col_idx += 1


def render_navigation_section(nav_links: List[Dict[str, Any]]) -> None:
    """Render a navigation section with links to related entities."""
    st.markdown("### 🔗 Related Navigation")
    if not nav_links:
        st.write("No navigation links available.")
        return

    cols = st.columns(len(nav_links))
    for i, link in enumerate(nav_links):
        with cols[i]:
            if st.button(link["label"], key=link.get("key", f"nav_{i}")):
                if "session_key" in link and "value" in link:
                    st.session_state[link["session_key"]] = link["value"]
                st.info(link["info"])


def render_notes_section(notes_df) -> None:
    """Render a notes section."""
    st.markdown("### 📝 Notes")
    if notes_df is None or notes_df.empty:
        st.write("No notes yet.")
        return

    for _, note_row in notes_df.iterrows():
        st.markdown(f"**{safe_text(note_row.get('author'))}** at {safe_text(note_row.get('created_at'))}:")
        st.write(safe_text(note_row.get('note')))
        st.divider()


def render_audit_section(audit_df, title: str = "Audit History") -> None:
    """Render an audit section."""
    st.markdown(f"### 📊 {title}")
    if audit_df is None or audit_df.empty:
        st.write("No audit events found (audit logging may not be enabled).")
        return

    for _, audit_row in audit_df.iterrows():
        event_type = safe_text(audit_row.get('event_type'))
        actor = safe_text(audit_row.get('actor'))
        created_at = safe_text(audit_row.get('created_at'))
        details = safe_text(audit_row.get('event_details'))

        st.markdown(f"**{event_type}** by {actor} at {created_at}:")
        st.write(details)
        st.divider()


def render_timeline_section(timeline_events: List[tuple]) -> None:
    """Render a timeline section."""
    st.markdown("### ⏰ Timeline")
    for label, timestamp in timeline_events:
        if timestamp:
            st.markdown(f"**{label}:** {safe_text(timestamp)}")


# Import pandas for safe_text/safe_val if needed
try:
    import pandas as pd
except ImportError:
    pd = None