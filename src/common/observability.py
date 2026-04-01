"""
Observability utilities for processing operations.

This module provides structured logging and metrics collection for processing runs,
enabling better monitoring and debugging of the eligibility and claims processing pipeline.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ProcessingMetrics:
    """Metrics collected during a processing run."""

    run_id: int
    file_id: int
    run_type: str
    rows_read: int
    rows_passed: int
    rows_failed: int
    issue_count: int
    start_time: float
    end_time: float | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Calculate processing duration in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.rows_read == 0:
            return 0.0
        return (self.rows_passed / self.rows_read) * 100.0

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate as percentage."""
        if self.rows_read == 0:
            return 0.0
        return (self.rows_failed / self.rows_read) * 100.0

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for logging/serialization."""
        return {
            "run_id": self.run_id,
            "file_id": self.file_id,
            "run_type": self.run_type,
            "rows_read": self.rows_read,
            "rows_passed": self.rows_passed,
            "rows_failed": self.rows_failed,
            "issue_count": self.issue_count,
            "duration_seconds": self.duration_seconds,
            "success_rate": self.success_rate,
            "failure_rate": self.failure_rate,
        }


class ProcessingLogger:
    """Structured logger for processing operations."""

    @staticmethod
    def log_run_start(run_id: int, file_id: int, run_type: str, file_path: str) -> None:
        """Log the start of a processing run."""
        logger.info(
            "Processing run started",
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "run_type": run_type,
                "file_path": str(file_path),
                "event": "run_start",
            }
        )

    @staticmethod
    def log_run_complete(metrics: ProcessingMetrics, status: str) -> None:
        """Log the completion of a processing run with metrics."""
        logger.info(
            f"Processing run completed with status: {status}",
            extra={
                "event": "run_complete",
                "status": status,
                **metrics.to_dict(),
            }
        )

    @staticmethod
    def create_metrics_from_finalize(
        run_id: int, file_id: int, run_type: str, rows_read: int, rows_passed: int, rows_failed: int, issue_count: int, start_time: float | None = None
    ) -> ProcessingMetrics:
        """Create metrics from finalization parameters."""
        metrics = create_processing_metrics(run_id, file_id, run_type, rows_read, rows_passed, rows_failed, issue_count, start_time)
        return finalize_processing_metrics(metrics)

    @staticmethod
    def log_run_error(run_id: int, file_id: int, error: Exception) -> None:
        """Log a processing run error."""
        logger.error(
            "Processing run failed",
            exc_info=error,
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "event": "run_error",
                "error_type": type(error).__name__,
            }
        )

    @staticmethod
    def log_batch_validation_complete(
        run_id: int, file_id: int, rows_read: int, batch_issue_rows: int
    ) -> None:
        """Log completion of batch validation."""
        logger.info(
            "Batch validation completed",
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "event": "batch_validation_complete",
                "rows_read": rows_read,
                "batch_issue_rows": batch_issue_rows,
            }
        )

    @staticmethod
    def log_row_processed(
        run_id: int, file_id: int, row_number: int, success: bool, issue_count: int = 0
    ) -> None:
        """Log processing of individual row."""
        logger.debug(
            f"Row processed: {'success' if success else 'failed'}",
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "row_number": row_number,
                "event": "row_processed",
                "success": success,
                "issue_count": issue_count,
            }
        )

    @staticmethod
    def log_file_status_update(file_id: int, old_status: str, new_status: str) -> None:
        """Log file status changes."""
        logger.info(
            f"File status updated: {old_status} -> {new_status}",
            extra={
                "file_id": file_id,
                "event": "file_status_update",
                "old_status": old_status,
                "new_status": new_status,
            }
        )

    @staticmethod
    def log_snapshot_rebuild(run_id: int, file_id: int, snapshots_rebuilt: int) -> None:
        """Log accumulator snapshot rebuild."""
        logger.info(
            "Accumulator snapshots rebuilt",
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "event": "snapshot_rebuild",
                "snapshots_rebuilt": snapshots_rebuilt,
            }
        )

    @staticmethod
    def log_anomaly_detection(run_id: int, file_id: int, anomalies_found: int) -> None:
        """Log anomaly detection results."""
        logger.info(
            "Anomaly detection completed",
            extra={
                "run_id": run_id,
                "file_id": file_id,
                "event": "anomaly_detection",
                "anomalies_found": anomalies_found,
            }
        )


def create_processing_metrics(
    run_id: int, file_id: int, run_type: str, rows_read: int, rows_passed: int, rows_failed: int, issue_count: int, start_time: float | None = None
) -> ProcessingMetrics:
    """Create a ProcessingMetrics instance with current timestamp."""
    return ProcessingMetrics(
        run_id=run_id,
        file_id=file_id,
        run_type=run_type,
        rows_read=rows_read,
        rows_passed=rows_passed,
        rows_failed=rows_failed,
        issue_count=issue_count,
        start_time=start_time or time.time(),
    )


def finalize_processing_metrics(metrics: ProcessingMetrics) -> ProcessingMetrics:
    """Mark processing metrics as complete with end time."""
    metrics.end_time = time.time()
    return metrics