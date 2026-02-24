"""Structured logging, alerting, metrics, and profiling helpers for ETL jobs."""

import json
import logging
import smtplib
import time
import traceback
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Dict, List

import requests
from pythonjsonlogger import jsonlogger

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severities used by pipeline monitoring notifications."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class PipelineLogger:
    """Emit JSON-structured task and data-quality logs."""

    def __init__(self, name: str, log_file: str = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        if self.logger.handlers:
            return

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(jsonlogger.JsonFormatter())
        self.logger.addHandler(console_handler)

        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(jsonlogger.JsonFormatter())
            self.logger.addHandler(file_handler)

    def log_task_start(self, task_name: str, params: Dict = None) -> None:
        self.logger.info(
            "task_started",
            extra={
                "task_name": task_name,
                "timestamp": datetime.now().isoformat(),
                "params": params or {},
            },
        )

    def log_task_end(
        self,
        task_name: str,
        status: str,
        duration_seconds: float,
        result: Dict = None,
    ) -> None:
        self.logger.info(
            "task_completed",
            extra={
                "task_name": task_name,
                "status": status,
                "duration_seconds": duration_seconds,
                "timestamp": datetime.now().isoformat(),
                "result": result or {},
            },
        )

    def log_data_quality(
        self,
        stage: str,
        record_count: int,
        quality_score: float,
        issues: List[str] = None,
    ) -> None:
        self.logger.info(
            "data_quality_check",
            extra={
                "stage": stage,
                "record_count": record_count,
                "quality_score": quality_score,
                "issue_count": len(issues or []),
                "issues": issues or [],
                "timestamp": datetime.now().isoformat(),
            },
        )

    def log_error(
        self,
        task_name: str,
        error: Exception,
        context: Dict = None,
    ) -> None:
        self.logger.error(
            "task_failed",
            extra={
                "task_name": task_name,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "stacktrace": traceback.format_exc(),
                "context": context or {},
                "timestamp": datetime.now().isoformat(),
            },
        )


class AnomalyAlert:
    """Send anomaly notifications to Slack and/or email."""

    def __init__(self, slack_webhook_url: str = None, email_config: Dict = None):
        self.slack_webhook_url = slack_webhook_url
        self.email_config = email_config or {}
        self.logger = logging.getLogger(__name__)

    def trigger_alert(
        self,
        level: AlertLevel,
        title: str,
        message: str,
        context: Dict = None,
    ) -> None:
        alert_data = {
            "level": level.value,
            "title": title,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "context": context or {},
        }

        self.logger.warning(f"ALERT [{level.value}] {title}: {message}")

        if self.slack_webhook_url:
            self._send_slack_alert(alert_data)

        if self.email_config.get("enabled"):
            self._send_email_alert(alert_data)

    def _send_slack_alert(self, alert: Dict) -> None:
        try:
            color_map = {
                "INFO": "#36a64f",
                "WARNING": "#ff9900",
                "ERROR": "#ff0000",
                "CRITICAL": "#8b0000",
            }

            payload = {
                "attachments": [
                    {
                        "color": color_map.get(alert["level"], "#808080"),
                        "title": alert["title"],
                        "text": alert["message"],
                        "fields": [
                            {
                                "title": "Severity",
                                "value": alert["level"],
                                "short": True,
                            },
                            {
                                "title": "Time",
                                "value": alert["timestamp"],
                                "short": True,
                            },
                        ],
                    },
                ],
            }

            response = requests.post(self.slack_webhook_url, json=payload)
            if response.status_code == 200:
                self.logger.info("Alert sent to Slack successfully")
            else:
                self.logger.warning(f"Failed to send Slack alert: {response.text}")

        except Exception as e:
            self.logger.error(f"Error sending Slack alert: {e}")

    def _send_email_alert(self, alert: Dict) -> None:
        try:
            smtp_server = self.email_config.get("smtp_server", "smtp.gmail.com")
            smtp_port = self.email_config.get("smtp_port", 587)
            sender_email = self.email_config.get("sender_email")
            sender_password = self.email_config.get("sender_password")
            recipient_emails = self.email_config.get("recipient_emails", [])

            if not all([sender_email, sender_password, recipient_emails]):
                self.logger.warning("Email configuration incomplete")
                return

            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = ", ".join(recipient_emails)
            msg["Subject"] = f"[{alert['level']}] {alert['title']}"

            body = f"""
            Anomaly Alert

            Title: {alert['title']}
            Severity: {alert['level']}
            Time: {alert['timestamp']}

            Message:
            {alert['message']}

            Context:
            {json.dumps(alert['context'], indent=2)}
            """

            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, sender_password)
                server.send_message(msg)

            self.logger.info("Alert sent via email successfully")

        except Exception as e:
            self.logger.error(f"Error sending email alert: {e}")


class PipelineMonitor:
    """In-memory metric recorder with threshold checks."""

    def __init__(self):
        self.metrics = {}
        self.logger = logging.getLogger(__name__)

    def record_metric(
        self,
        metric_name: str,
        value: float,
        metadata: Dict = None,
    ) -> None:
        timestamp = datetime.now().isoformat()

        if metric_name not in self.metrics:
            self.metrics[metric_name] = []

        self.metrics[metric_name].append(
            {
                "value": value,
                "timestamp": timestamp,
                "metadata": metadata or {},
            },
        )

    def get_metrics_summary(self) -> Dict[str, Any]:
        summary = {}

        for metric_name, data_points in self.metrics.items():
            values = [point["value"] for point in data_points]
            summary[metric_name] = {
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
                "latest": values[-1] if values else None,
                "timestamp": data_points[-1]["timestamp"] if data_points else None,
            }

        return summary

    def check_performance_threshold(
        self,
        metric_name: str,
        threshold: float,
        operator: str = ">",
    ) -> bool:
        if metric_name not in self.metrics or not self.metrics[metric_name]:
            return False

        latest_value = self.metrics[metric_name][-1]["value"]

        if operator == ">":
            return latest_value > threshold
        elif operator == "<":
            return latest_value < threshold
        elif operator == "==":
            return latest_value == threshold
        elif operator == "!=":
            return latest_value != threshold
        elif operator == ">=":
            return latest_value >= threshold
        elif operator == "<=":
            return latest_value <= threshold

        return False


class PerformanceProfiler:
    """Task-level runtime profiler with bottleneck summaries."""

    def __init__(self):
        self.profiles = {}
        self.history = {}
        self.logger = logging.getLogger(__name__)

    def start_profiling(self, task_name: str) -> None:
        self.profiles[task_name] = {
            "start_time": datetime.now(),
            "start_perf_counter": time.perf_counter(),
            "end_time": None,
            "duration_seconds": None,
            "status": "running",
        }

    def end_profiling(self, task_name: str, status: str = "completed") -> float:
        if task_name not in self.profiles:
            self.logger.warning(f"No profiling started for {task_name}")
            return 0

        end_time = datetime.now()
        start_perf_counter = self.profiles[task_name].get("start_perf_counter")

        if start_perf_counter is not None:
            duration = time.perf_counter() - start_perf_counter
        else:
            duration = (
                end_time - self.profiles[task_name]["start_time"]
            ).total_seconds()

        self.profiles[task_name]["end_time"] = end_time
        self.profiles[task_name]["duration_seconds"] = duration
        self.profiles[task_name]["status"] = status

        if task_name not in self.history:
            self.history[task_name] = []

        self.history[task_name].append(
            {
                "end_time": end_time,
                "duration_seconds": duration,
                "status": status,
            },
        )

        self.logger.info(
            f"Task {task_name} completed in {duration:.2f} seconds "
            f"(status: {status})",
        )

        return duration

    def get_bottlenecks(
        self,
        top_n: int = 3,
        min_duration_seconds: float = 0.0,
    ) -> List[Dict[str, Any]]:
        bottlenecks = []

        for task_name, runs in self.history.items():
            durations = [
                run["duration_seconds"] for run in runs if run["duration_seconds"]
            ]

            if not durations:
                continue

            avg_duration = sum(durations) / len(durations)
            if avg_duration < min_duration_seconds:
                continue

            bottlenecks.append(
                {
                    "task_name": task_name,
                    "avg_duration_seconds": avg_duration,
                    "max_duration_seconds": max(durations),
                    "latest_duration_seconds": durations[-1],
                    "run_count": len(durations),
                },
            )

        bottlenecks.sort(
            key=lambda item: (
                item["avg_duration_seconds"],
                item["max_duration_seconds"],
            ),
            reverse=True,
        )

        return bottlenecks[:top_n]

    def get_profile_summary(self) -> Dict[str, Any]:
        summary = {
            "total_tasks": len(self.profiles),
            "total_duration": 0,
            "tasks": {},
            "bottlenecks": self.get_bottlenecks(top_n=5),
        }

        for task_name, profile in self.profiles.items():
            duration = profile["duration_seconds"] or 0
            summary["total_duration"] += duration

            task_runs = self.history.get(task_name, [])
            run_durations = [run["duration_seconds"] for run in task_runs]
            avg_duration = (
                (sum(run_durations) / len(run_durations)) if run_durations else duration
            )

            summary["tasks"][task_name] = {
                "duration_seconds": duration,
                "avg_duration_seconds": avg_duration,
                "run_count": len(task_runs) if task_runs else 1,
                "status": profile["status"],
            }

        return summary
