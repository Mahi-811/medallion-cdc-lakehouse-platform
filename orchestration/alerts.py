"""
Alerting and Notification Hooks for Data Orchestration.
Dispatches structured failure alerts to Slack/Teams/PagerDuty webhooks.
"""
from __future__ import annotations
import os
import json
import logging
from datetime import datetime
from typing import Any
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OrchestrationAlerts")

WEBHOOK_URL = os.getenv("DATA_PLATFORM_ALERT_WEBHOOK", "https://mock.webhook.office.com/webhook/alerts")

def on_task_failure_alert(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback. Formats rich diagnostics about the failed task
    and sends a notification to the engineering alert channel.
    """
    ti = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown_task"
    dag_id = ti.dag_id if ti else "unknown_dag"
    execution_date = context.get("execution_date", datetime.utcnow()).isoformat()
    log_url = ti.log_url if ti else "N/A"
    exception = context.get("exception", "No exception message provided")

    payload = {
        "text": f":rotating_light: *Pipeline Task Failure Alert* :rotating_light:",
        "attachments": [
            {
                "color": "#D32F2F",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {"title": "Execution Date", "value": execution_date, "short": True},
                    {"title": "Try Number", "value": str(ti.try_number if ti else 1), "short": True},
                    {"title": "Error Message", "value": str(exception)[:300], "short": False},
                    {"title": "Log URL", "value": f"<{log_url}|View Task Logs>", "short": False}
                ],
                "footer": "Medallion Data Platform Orchestrator",
                "ts": int(datetime.utcnow().timestamp())
            }
        ]
    }

    logger.error("TASK FAILURE: DAG=%s, Task=%s, Error=%s", dag_id, task_id, exception)

    if WEBHOOK_URL and not WEBHOOK_URL.startswith("https://mock"):
        try:
            resp = requests.post(WEBHOOK_URL, json=payload, timeout=5)
            logger.info("Alert dispatched to webhook: status %d", resp.status_code)
        except Exception as e:
            logger.warning("Failed to dispatch webhook alert: %s", e)
    else:
        logger.info("Alert payload generated successfully (mock webhook configured): %s", json.dumps(payload, indent=2))
