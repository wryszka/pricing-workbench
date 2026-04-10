"""Async audit logging wrapper for the FastAPI app.

Thin layer over the shared audit utility that uses the app's SQL executor
and config for catalog/schema resolution.
"""

import json
import uuid
import logging
from datetime import datetime, timezone

from server.config import fqn, get_current_user
from server.sql import execute_query

logger = logging.getLogger(__name__)

AUDIT_TABLE = "audit_log"


def _escape(val: str) -> str:
    return val.replace("'", "''")


async def log_audit_event(
    event_type: str,
    entity_type: str,
    entity_id: str,
    entity_version: str = "",
    user_id: str | None = None,
    details: dict | str | None = None,
    source: str = "app",
) -> str | None:
    """Log an audit event from the FastAPI app. Returns the event_id or None on failure."""
    event_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    resolved_user = user_id or get_current_user()

    if details is None:
        details_str = "{}"
    elif isinstance(details, dict):
        details_str = json.dumps(details)
    else:
        details_str = str(details)

    sql = f"""
        INSERT INTO {fqn(AUDIT_TABLE)} VALUES (
            '{event_id}',
            '{_escape(event_type)}',
            '{_escape(entity_type)}',
            '{_escape(entity_id)}',
            '{_escape(entity_version)}',
            '{_escape(resolved_user)}',
            '{now}',
            '{_escape(details_str)}',
            '{_escape(source)}'
        )
    """

    try:
        await execute_query(sql)
        logger.info("Audit: %s %s/%s by %s", event_type, entity_type, entity_id, resolved_user)
        return event_id
    except Exception:
        logger.exception("Failed to log audit event %s for %s/%s", event_type, entity_type, entity_id)
        return None
