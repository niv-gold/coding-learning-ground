"""Snowflake-backed system event logging implementation.

This module implements `SnowflakeLoadLogRepository`, which writes
and queries events in a Snowflake `SYSTEM_EVENT_LOG` table. It is used
by the ingestion pipeline to record run-level and per-file ingest
events. Methods accept explicit `event_id` values so callers can
correlate related events.

Main responsibilities (simple):
 - Check which files were already ingested (`already_loaded`).
 - Write structured events for run lifecycle and per-file ingests.

Note: This is a lightweight helper intended for demos and tests. In
production you may want connection pooling, retries, and better error
handling.
"""

from __future__ import annotations
from snowflake import connector
from nyc_taxi.ingestion.core.ports import LoadLogRepository
from nyc_taxi.ingestion.config.settings import SnowflakeConfig
from nyc_taxi.ingestion.infra.local_finder import LocalFileFinder
import uuid
from pathlib import Path
import json
from dotenv import load_dotenv


class SnowflakeLoadLogRepository(LoadLogRepository):
    """Persist and query pipeline events in Snowflake.

    Args:
        conn_params (dict): Connection parameters for Snowflake; expected
            to include `log_table` and `log_schema` keys indicating where
            to write events.

    Main use (simple):
        - `already_loaded(file_keys)` returns which files were already
          ingested successfully.
        - `log_ingest_success` / `log_ingest_failure` record per-file events.
        - `log_run_started` / `log_run_finished` record run lifecycle events.
    """

    def __init__(self, conn_params: dict):
        self.conn_params = conn_params
        self.log_table = conn_params["log_table"]
        self.log_schema = conn_params["log_schema"]

    def _connect(self):
        """Return a Snowflake connection using `conn_params`.

        Returns:
            snowflake.connector.SnowflakeConnection
        """
        return connector.connect(**self.conn_params)

    def already_loaded(self, file_keys: list[str]) -> set[str]:
        """Return which stable `file_keys` have prior successful ingest events.

        Args:
            file_keys (list[str]): Stable keys to check. Stable key format
                is typically `name|size_bytes|timestamp`.

        Returns:
            set[str]: Subset of provided keys that have a SUCCESS ingest record.
        """

        if not file_keys:
            return set()

        # Build placeholders for VALUES binding to avoid SQL injection
        sql_place_holders = ", ".join(["(%s)"] * len(file_keys))
        sql = f"""
            SELECT distinct entity_id
            FROM {self.log_schema}.{self.log_table}
            WHERE entity_id IN (SELECT column1 FROM VALUES {sql_place_holders})
                AND entity_type = 'FILE'
                AND event_type  = 'INGEST'
                AND status = 'SUCCESS'
        """

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, file_keys)
                rows = cur.fetchall()

        # rows are tuples like (entity_id,); return a set of entity_id strings
        return {r[0] for r in rows}

    def _insert_event(
        self,
        *,
        run_id: str,
        event_id: str,
        event_level: str,
        event_type: str,
        component: str,
        entity_type: str,
        entity_id: str,
        status: str,
        message: str,
        error_code: str | None,
        error_details: str | None,
        metadata: dict,
    ) -> None:
        """Insert a structured event row into the system log table.

        Args:
            run_id (str): Identifier for the pipeline run.
            event_id (str): Unique identifier for this event.
            event_level (str): Severity level (e.g., 'INFO', 'ERROR').
            event_type (str): Logical event type (e.g., 'RUN', 'INGEST').
            component (str): Component name that emitted the event.
            entity_type (str): Type of entity (e.g., 'FILE', 'RUN').
            entity_id (str): Entity identifier (e.g., stable file key).
            status (str): Status string (e.g., 'SUCCESS' or 'FAILURE').
            message (str): Human-readable message.
            error_code (str|None): Optional machine-readable error code.
            error_details (str|None): Optional detailed error text.
            metadata (dict): Arbitrary JSON-serializable metadata.

        Returns:
            None
        """

        metadata_json = json.dumps(metadata or {}, allow_nan=False, separators=(",", ":"), ensure_ascii=False)

        sql = f"""
            INSERT INTO {self.log_schema}.{self.log_table} (
                run_id,
                event_id,                
                event_timestamp,
                event_level,
                event_type,
                component,
                entity_type,
                entity_id,
                status,
                message,
                error_code,
                error_details,
                metadata
            ) 
            SELECT
            %s, %s, CURRENT_TIMESTAMP(),
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                PARSE_JSON(%s)            
            """

        params = (
            run_id,
            event_id,
            event_level,
            event_type,
            component,
            entity_type,
            entity_id,
            status,
            message,
            error_code,
            error_details,
            metadata_json,
        )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def log_run_started(self, *, run_id: str, event_id: str, component: str, message: str, metadata: dict | None = None) -> None:
        """Record that a pipeline run has started.

        Args:
            run_id (str): Pipeline run id.
            event_id (str): Unique event id (caller-generated).
            component (str): Component name emitting the event.
            message (str): Human-readable message.
            metadata (dict|None): Optional additional data.
        """
        self._insert_event(
            run_id=run_id,
            event_id=event_id,
            event_level="INFO",
            event_type="RUN",
            component=component,
            entity_type="RUN",
            entity_id=run_id,
            status="STARTED",
            message=message,
            error_code=None,
            error_details=None,
            metadata=metadata,
        )

    def log_run_finished(self, *, run_id: str, event_id: str, component: str, status: str, message: str, metadata: dict | None = None) -> None:
        """Record that a pipeline run has finished.

        Args:
            run_id (str): Pipeline run id.
            event_id (str): Unique event id (caller-generated).
            component (str): Component name emitting the event.
            status (str): Final status, usually 'SUCCESS' or 'FAILURE'.
            message (str): Human-readable message.
            metadata (dict|None): Optional additional data.
        """
        # status should be "SUCCESS" or "FAILURE" for the overall run
        self._insert_event(
            run_id=run_id,
            event_id=event_id,
            event_level="INFO" if status == "SUCCESS" else "ERROR",
            event_type="RUN",
            component=component,
            entity_type="RUN",
            entity_id=run_id,
            status=status,
            message=message,
            error_code=None,
            error_details=None,
            metadata=metadata,
        )

    def log_success(self, run_id: str, entity_type: str, entity_id: str, component: str, message: str, metadata: dict) -> None:
        """Deprecated generic success logger. Prefer `log_ingest_success` with `event_id`."""
        raise NotImplementedError("Use log_ingest_success(...) with event_id or extend this method to generate event_id")

    def log_failure(self, run_id: str, entity_type: str, entity_id: str, component: str, message: str, error_details: str, metadata: dict) -> None:
        """Deprecated generic failure logger. Prefer `log_ingest_failure` with `event_id`."""
        raise NotImplementedError("Use log_ingest_failure(...) with event_id or extend this method to generate event_id")

    # Practical methods — explicit event_id for correlation
    def log_ingest_success(self, *, run_id: str, event_id: str, component: str, entity_id: str, message: str, metadata: dict) -> None:
        """Log a successful file ingestion event.

        Args:
            run_id (str): Pipeline run id.
            event_id (str): Unique event id.
            component (str): Component name (e.g., 'INGESTION').
            entity_id (str): File stable key.
            message (str): Human-readable message.
            metadata (dict): Additional structured metadata.
        """
        self._insert_event(
            run_id=run_id,
            event_id=event_id,
            event_level="INFO",
            event_type="INGEST",
            component=component,
            entity_type="FILE",
            entity_id=entity_id,
            status="SUCCESS",
            message=message,
            error_code=None,
            error_details=None,
            metadata=metadata,
        )

    def log_ingest_failure(self, *, run_id: str, event_id: str, component: str, entity_id: str, message: str, error_details: str, metadata: dict) -> None:
        """Log a failed file ingestion event.

        Args:
            run_id (str): Pipeline run id.
            event_id (str): Unique event id.
            component (str): Component name.
            entity_id (str): File stable key.
            message (str): Human-readable message.
            error_details (str): Detailed error information.
            metadata (dict): Additional structured metadata.
        """
        self._insert_event(
            run_id=run_id,
            event_id=event_id,
            event_level="ERROR",
            event_type="INGEST",
            component=component,
            entity_type="FILE",
            entity_id=entity_id,
            status="FAILURE",
            message=message,
            error_code=None,
            error_details=error_details,
            metadata=metadata,
        )

    def log_run_event(self, *, run_id: str, event_id: str, component: str, entity_id: str, message: str, metadata: dict) -> None:
        """Log a generic run-level informational event.
        Args:
            run_id (str): Pipeline run id.
            event_id (str): Unique event id.
            component (str): Component name.
            entity_id (str): Run identifier.
            message (str): Human-readable message.
            metadata (dict): Additional structured metadata.

        This is a helper to record arbitrary pipeline-level messages.
        """
        self._insert_event(
            run_id=run_id,
            event_id=event_id,
            event_level="INFO",
            event_type="PIPELINE",
            component=component,
            entity_type="RUN",
            entity_id=entity_id,
            status="SUCCESS",
            message=message,
            error_code=None,
            error_details=None,
            metadata=metadata,
        )


if __name__ == '__main__':
    # Quick manual test harness (requires a .env with Snowflake credentials)
    load_dotenv()

    path = Path('/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/ingestion/app/data_files')
    files = LocalFileFinder(path).list_files()
    list_files_stable_key = [file.stable_key for file in files]

    snowflake_config = SnowflakeConfig.from_env().to_connector_kwarg()
    sn_inst = SnowflakeLoadLogRepository(conn_params=snowflake_config)

    sn_inst._insert_event(
        run_id=str(uuid.uuid4()),
        event_id=str(uuid.uuid4()),        
        event_level='INFO',
        event_type='RUN',
        component='INGESTION',
        entity_type='FILE',
        entity_id='taxi_zone_lookup.csv|12345|1767285799',
        status='SUCCESS',
        message='The log is A OK',
        error_code=None,
        error_details=None,
        metadata={"stage": "test_step"},
    )

    sn_inst.log_ingest_success(
        run_id=str(uuid.uuid4()),
        event_id=str(uuid.uuid4()),        
        component='INGESTION',
        entity_id='taxi_zone_lookup.csv|12345|1767285799',
        message='sn_inst.log_ingest_success',
        metadata={"stage": "test_step_1"},
    )

    sn_inst.log_ingest_failure(
        run_id=str(uuid.uuid4()),
        event_id=str(uuid.uuid4()),        
        component='INGESTION',
        entity_id='taxi_zone_lookup.csv|12345|1767285799',
        message='sn_inst.log_ingest_failure',
        error_details='Error as occurred!!!',
        metadata={"stage": "test_step_1"},
    )