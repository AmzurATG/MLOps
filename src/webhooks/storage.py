"""
Webhook Event Storage
=====================

Unified Trino storage for webhook events across all domains.
"""

import re
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from trino.dbapi import connect

from src.webhooks.config import WebhookConfig, get_config
from src.webhooks.models import LakeFSEvent


class WebhookStorage:
    """
    Unified storage for webhook events.

    Stores events in domain-specific tables with a consistent schema.
    """

    def __init__(self, config: WebhookConfig = None):
        self.config = config or get_config()

    def get_connection(self, schema: str = "metadata"):
        """Get Trino connection."""
        return connect(
            host=self.config.trino_host,
            port=self.config.trino_port,
            user=self.config.trino_user,
            catalog=self.config.trino_catalog,
            schema=schema,
        )

    def ensure_table_exists(self, domain: str) -> bool:
        """Ensure the events table exists for a domain."""
        table = self.config.get_events_table(domain)
        schema = self.config.get_schema_for_domain(domain)

        conn = self.get_connection(schema)
        cursor = conn.cursor()

        try:
            # Check if table exists
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}'
                  AND table_name = '{self.config.events_table}'
            """)

            if cursor.fetchone()[0] == 0:
                print(f"Creating {table} table...")

                # Create table with unified schema
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id BIGINT,
                        event_id VARCHAR,
                        event_type VARCHAR,
                        domain VARCHAR,
                        repository VARCHAR,
                        branch VARCHAR,
                        commit_id VARCHAR,
                        reverted_commit VARCHAR,
                        source_ref VARCHAR,
                        committer VARCHAR,
                        message VARCHAR,
                        payload JSON,
                        received_at TIMESTAMP(6) WITH TIME ZONE,
                        processed_at TIMESTAMP(6) WITH TIME ZONE,
                        status VARCHAR,
                        error_message VARCHAR,
                        retry_count INTEGER,
                        metadata JSON
                    )
                """)
                print(f"Created {table} table")
                return True

            return True

        except Exception as e:
            print(f"Warning: Could not ensure table exists: {e}")
            return False
        finally:
            cursor.close()
            conn.close()

    def extract_reverted_commit(
        self, event: LakeFSEvent
    ) -> Optional[str]:
        """Extract the reverted commit ID from a post-revert event."""
        if event.event_type != "post-revert":
            return None

        # Try to extract from commit_message: "Revert <commit_id>"
        if event.commit_message:
            match = re.search(r"Revert\s+([a-f0-9]{64})", event.commit_message)
            if match:
                return match.group(1)

        # Fallback: use source_ref if different from commit_id
        if event.source_ref and event.source_ref != event.commit_id:
            return event.source_ref

        return None

    def store_event(
        self,
        event: LakeFSEvent,
        domain: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Store a webhook event.

        Args:
            event: The LakeFS event
            domain: Domain identifier (mlops, cvops, llmops)
            metadata: Optional domain-specific metadata

        Returns:
            The generated event ID
        """
        table = self.config.get_events_table(domain)
        schema = self.config.get_schema_for_domain(domain)

        conn = self.get_connection(schema)
        cursor = conn.cursor()

        try:
            # Extract event fields
            event_id = event.event_id or str(datetime.now().timestamp())
            reverted_commit = self.extract_reverted_commit(event) or ""

            # Escape function
            def escape(s):
                return str(s).replace("'", "''") if s else ""

            # Prepare values
            event_id_esc = escape(event_id)
            event_type_esc = escape(event.event_type)
            domain_esc = escape(domain)
            repository_esc = escape(event.repository_id)
            branch_esc = escape(event.branch_id)
            commit_id_esc = escape(event.commit_id)
            reverted_commit_esc = escape(reverted_commit)
            source_ref_esc = escape(event.source_ref)
            committer_esc = escape(event.committer)
            message_esc = escape(event.commit_message)

            # JSON payloads
            payload_json = json.dumps(event.model_dump()).replace("'", "''")
            metadata_json = json.dumps(metadata or {}).replace("'", "''")

            # Generate ID
            cursor.execute(f"""
                SELECT COALESCE(MAX(id), 0) + 1 FROM {table}
            """)
            new_id = cursor.fetchone()[0]

            # Insert event with explicit column names
            insert_sql = f"""
            INSERT INTO {table} (
                id,
                event_id,
                event_type,
                domain,
                repository,
                branch,
                commit_id,
                reverted_commit_id,
                source_ref,
                committer,
                message,
                payload,
                received_at,
                processed_at,
                status,
                error_message,
                retry_count,
                metadata
            )
            VALUES (
                {new_id},
                '{event_id_esc}',
                '{event_type_esc}',
                '{domain_esc}',
                '{repository_esc}',
                '{branch_esc}',
                '{commit_id_esc}',
                '{reverted_commit_esc}',
                '{source_ref_esc}',
                '{committer_esc}',
                '{message_esc}',
                '{payload_json}',
                CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)),
                CAST(NULL AS TIMESTAMP(6)),
                'pending',
                CAST(NULL AS VARCHAR),
                0,
                '{metadata_json}'
            )
            """

            cursor.execute(insert_sql)

            print(f"Stored {domain} webhook event: {event_id} ({event.event_type}) [id={new_id}]")

            return new_id

        except Exception as e:
            print(f"Failed to store webhook event: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def get_pending_events(
        self, domain: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get pending webhook events for a domain."""
        table = self.config.get_events_table(domain)
        schema = self.config.get_schema_for_domain(domain)

        conn = self.get_connection(schema)
        cursor = conn.cursor()

        try:
            cursor.execute(f"""
                SELECT id, event_type, repository, branch, commit_id,
                       reverted_commit, received_at
                FROM {table}
                WHERE status = 'pending'
                ORDER BY received_at ASC
                LIMIT {limit}
            """)

            events = []
            for row in cursor.fetchall():
                events.append({
                    "id": row[0],
                    "event_type": row[1],
                    "repository": row[2],
                    "branch": row[3],
                    "commit_id": row[4],
                    "reverted_commit": row[5],
                    "received_at": row[6].isoformat() if row[6] else None,
                })

            return events

        except Exception as e:
            print(f"Failed to get pending events: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_processed(
        self,
        domain: str,
        event_id: int,
        status: str = "processed",
        error_message: Optional[str] = None,
    ) -> bool:
        """Mark an event as processed."""
        table = self.config.get_events_table(domain)
        schema = self.config.get_schema_for_domain(domain)

        conn = self.get_connection(schema)
        cursor = conn.cursor()

        try:
            error_esc = (error_message or "").replace("'", "''")

            cursor.execute(f"""
                UPDATE {table}
                SET status = '{status}',
                    processed_at = CURRENT_TIMESTAMP,
                    error_message = CASE
                        WHEN '{error_esc}' = '' THEN NULL
                        ELSE '{error_esc}'
                    END
                WHERE id = {event_id}
            """)

            return True

        except Exception as e:
            print(f"Failed to mark event processed: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
