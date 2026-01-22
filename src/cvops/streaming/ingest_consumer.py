#!/usr/bin/env python3
"""
CVOps Kafka Ingestion Consumer
===============================================================================

Real-time consumer for CVOps image ingestion from Kafka.

ARCHITECTURE:
    External Sources (IP Cameras, Edge Devices, Batch Scan)
        → MinIO/S3 (image upload)
            → Kafka (cv.images.ingest - metadata only)
                → This Consumer
                    → Validate metadata
                    → Copy to LakeFS (if needed)
                    → Commit to LakeFS
                    → Insert to Iceberg cv.image_metadata
                    → Publish to cv.images.validated
                    → On error: publish to cv.images.dlq

MESSAGE FORMAT (cv.images.ingest):
    {
        "image_id": "uuid-string",
        "storage_path": "s3://cv-raw/source_id/2025/12/17/image.jpg",
        "source_type": "ip_camera | edge_device | batch_scan | api_upload",
        "source_id": "camera-01",
        "timestamp": "2025-12-17T10:30:00Z",
        "metadata": {"width": 1920, "height": 1080, "format": "jpg", ...},
        "batch_id": "null | batch-uuid",
        "priority": "high | normal | low"
    }

Usage:
    python cv_ingest_consumer.py [options]

    Options:
        --lakefs-server http://exp-lakefs:8000   LakeFS server URL
        --repo cv-data                           LakeFS repository
        --branch dev-cvops                       LakeFS branch
        --batch-size 100                         Iceberg insert batch size
        --no-lakefs-copy                         Skip LakeFS copy (images already in LakeFS)

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS    Kafka broker (default: exp-kafka:9092)
    CVOPS_KAFKA_INGEST_TOPIC   Topic to consume (default: cv.images.ingest)
    CVOPS_KAFKA_CONSUMER_GROUP Consumer group (default: cvops-ingest-consumer)
    LAKEFS_SERVER              LakeFS server URL
    LAKEFS_ACCESS_KEY_ID       LakeFS access key
    LAKEFS_SECRET_ACCESS_KEY   LakeFS secret key
    CVOPS_REPO                 LakeFS repository (default: cv-data)
    CVOPS_BRANCH               LakeFS branch (default: dev-cvops)
    TRINO_HOST                 Trino host (default: exp-trino)
    TRINO_PORT                 Trino port (default: 8080)
    MINIO_ENDPOINT             MinIO endpoint (default: http://exp-minio:9000)
"""

import os
import sys
import json
import signal
import time
import uuid
import logging
import argparse
import threading
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from io import BytesIO

# ===============================================================================
# CONFIGURATION
# ===============================================================================

@dataclass
class Config:
    # Kafka
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
    kafka_ingest_topic: str = os.getenv("CVOPS_KAFKA_INGEST_TOPIC", "cv.images.ingest")
    kafka_dlq_topic: str = os.getenv("CVOPS_KAFKA_DLQ_TOPIC", "cv.images.dlq")
    kafka_validated_topic: str = os.getenv("CVOPS_KAFKA_VALIDATED_TOPIC", "cv.images.validated")
    kafka_consumer_group: str = os.getenv("CVOPS_KAFKA_CONSUMER_GROUP", "cvops-ingest-consumer")
    kafka_auto_offset_reset: str = os.getenv("CVOPS_KAFKA_AUTO_OFFSET_RESET", "earliest")
    kafka_max_poll_records: int = int(os.getenv("CVOPS_KAFKA_MAX_POLL_RECORDS", "500"))
    kafka_session_timeout_ms: int = int(os.getenv("CVOPS_KAFKA_SESSION_TIMEOUT", "30000"))

    # LakeFS
    lakefs_server: str = os.getenv("LAKEFS_SERVER", "http://exp-lakefs:8000")
    lakefs_access_key: str = os.getenv("LAKEFS_ACCESS_KEY_ID", "AKIAIOSFOLKFSSAMPLES")
    lakefs_secret_key: str = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
    lakefs_repo: str = os.getenv("CVOPS_REPO", "cv-data")
    lakefs_branch: str = os.getenv("CVOPS_BRANCH", "dev-cvops")
    lakefs_raw_prefix: str = os.getenv("CVOPS_RAW_PREFIX", "raw/images/")

    # MinIO
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://exp-minio:9000")
    minio_access_key: str = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "password123")

    # Trino / Iceberg
    trino_host: str = os.getenv("TRINO_HOST", "exp-trino")
    trino_port: int = int(os.getenv("TRINO_PORT", "8080"))
    trino_user: str = os.getenv("TRINO_USER", "trino")
    trino_catalog: str = os.getenv("TRINO_CATALOG", "iceberg_dev")
    image_metadata_table: str = os.getenv("CVOPS_IMAGE_METADATA_TABLE", "iceberg_dev.cv.image_metadata")

    # Processing
    batch_size: int = int(os.getenv("CVOPS_KAFKA_BATCH_SIZE", "100"))
    commit_interval_seconds: int = int(os.getenv("CVOPS_KAFKA_COMMIT_INTERVAL", "5"))
    copy_to_lakefs: bool = os.getenv("CVOPS_COPY_TO_LAKEFS", "true").lower() in ("true", "1", "yes")
    compute_quality_metrics: bool = os.getenv("CVOPS_COMPUTE_QUALITY", "false").lower() in ("true", "1", "yes")

    # Consumer identity
    consumer_id: str = field(default_factory=lambda: f"cv-ingest-{uuid.uuid4().hex[:8]}")

    verbose: bool = False


# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ===============================================================================
# LAKEFS CLIENT
# ===============================================================================

class LakeFSClient:
    """Simple LakeFS client for object operations."""

    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.lakefs_server.rstrip("/")
        self._session = None

    @property
    def session(self):
        if self._session is None:
            import requests
            self._session = requests.Session()
            self._session.auth = (self.config.lakefs_access_key, self.config.lakefs_secret_key)
        return self._session

    def object_exists(self, path: str) -> bool:
        """Check if object exists in LakeFS."""
        url = f"{self.base_url}/api/v1/repositories/{self.config.lakefs_repo}/refs/{self.config.lakefs_branch}/objects/stat"
        resp = self.session.get(url, params={"path": path}, timeout=10)
        return resp.status_code == 200

    def upload_object(self, path: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
        """Upload object to LakeFS."""
        url = f"{self.base_url}/api/v1/repositories/{self.config.lakefs_repo}/branches/{self.config.lakefs_branch}/objects"
        params = {"path": path}
        headers = {"Content-Type": content_type}
        resp = self.session.post(url, params=params, data=data, headers=headers, timeout=60)
        return resp.status_code in (200, 201)

    def commit(self, message: str, metadata: Dict[str, str] = None) -> Optional[str]:
        """Create a commit on the branch."""
        url = f"{self.base_url}/api/v1/repositories/{self.config.lakefs_repo}/branches/{self.config.lakefs_branch}/commits"
        payload = {
            "message": message,
            "metadata": metadata or {},
        }
        resp = self.session.post(url, json=payload, timeout=30)
        if resp.status_code in (200, 201):
            return resp.json().get("id")
        return None

    def get_uncommitted_changes(self) -> int:
        """Get count of uncommitted changes."""
        url = f"{self.base_url}/api/v1/repositories/{self.config.lakefs_repo}/branches/{self.config.lakefs_branch}/diff"
        resp = self.session.get(url, timeout=10)
        if resp.status_code == 200:
            results = resp.json().get("results", [])
            return len(results)
        return 0


# ===============================================================================
# MINIO CLIENT
# ===============================================================================

class MinIOClient:
    """Simple MinIO client for object operations."""

    def __init__(self, config: Config):
        self.config = config
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from minio import Minio
            from urllib.parse import urlparse
            parsed = urlparse(self.config.minio_endpoint)
            self._client = Minio(
                parsed.netloc,
                access_key=self.config.minio_access_key,
                secret_key=self.config.minio_secret_key,
                secure=parsed.scheme == "https",
            )
        return self._client

    def object_exists(self, bucket: str, key: str) -> bool:
        """Check if object exists in MinIO."""
        try:
            self.client.stat_object(bucket, key)
            return True
        except Exception:
            return False

    def get_object(self, bucket: str, key: str) -> Optional[bytes]:
        """Get object data from MinIO."""
        try:
            response = self.client.get_object(bucket, key)
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except Exception as e:
            logger.error(f"Failed to get object {bucket}/{key}: {e}")
            return None

    def get_object_info(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        """Get object metadata from MinIO."""
        try:
            stat = self.client.stat_object(bucket, key)
            return {
                "size": stat.size,
                "content_type": stat.content_type,
                "last_modified": stat.last_modified,
                "etag": stat.etag,
            }
        except Exception:
            return None


# ===============================================================================
# ICEBERG SINK
# ===============================================================================

class IcebergSink:
    """Batched writes to Iceberg for image metadata."""

    def __init__(self, config: Config):
        self.config = config
        self.buffer: List[Dict[str, Any]] = []
        self.buffer_lock = threading.Lock()
        self.last_flush = datetime.now()
        self._connection = None

    def _get_connection(self):
        if self._connection is None:
            from trino.dbapi import connect
            self._connection = connect(
                host=self.config.trino_host,
                port=self.config.trino_port,
                user=self.config.trino_user,
                catalog=self.config.trino_catalog,
            )
        return self._connection

    def _format_timestamp(self, ts) -> str:
        """Convert timestamp to Trino-compatible format."""
        if ts is None:
            return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(ts, datetime):
            return ts.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(ts, str):
            ts = ts.replace('T', ' ').replace('Z', '').split('.')[0]
            return ts
        return str(ts)

    def _escape_string(self, s: str) -> str:
        """Escape string for SQL."""
        if s is None:
            return "NULL"
        return "'" + str(s).replace("'", "''") + "'"

    def add(self, record: Dict[str, Any]):
        """Add a record to the buffer."""
        with self.buffer_lock:
            self.buffer.append(record)

    def should_flush(self) -> bool:
        """Check if buffer should be flushed."""
        if len(self.buffer) >= self.config.batch_size:
            return True
        elapsed = (datetime.now() - self.last_flush).total_seconds()
        return len(self.buffer) > 0 and elapsed >= self.config.commit_interval_seconds

    def flush(self) -> int:
        """Flush buffer to Iceberg."""
        with self.buffer_lock:
            if not self.buffer:
                return 0

            records = self.buffer.copy()
            self.buffer.clear()

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Build INSERT statement
            values_list = []
            for rec in records:
                values = f"""(
                    {self._escape_string(rec.get('image_id'))},
                    {self._escape_string(rec.get('original_path'))},
                    {self._escape_string(rec.get('storage_path'))},
                    {self._escape_string(rec.get('filename'))},
                    {rec.get('file_size', 0)},
                    {rec.get('width', 0)},
                    {rec.get('height', 0)},
                    {self._escape_string(rec.get('format', 'unknown'))},
                    {self._escape_string(rec.get('source_type'))},
                    {self._escape_string(rec.get('source_id'))},
                    TIMESTAMP '{self._format_timestamp(rec.get('captured_at'))}',
                    TIMESTAMP '{self._format_timestamp(rec.get('ingested_at'))}',
                    {rec.get('quality_score', 0.0)},
                    {rec.get('blur_score', 0.0)},
                    {rec.get('brightness', 0.0)},
                    {rec.get('contrast', 0.0)},
                    {self._escape_string(rec.get('perceptual_hash'))},
                    false,
                    {self._escape_string(rec.get('source_lakefs_commit'))},
                    {self._escape_string(rec.get('source_lakefs_branch'))}
                )"""
                values_list.append(values)

            sql = f"""
                INSERT INTO {self.config.image_metadata_table}
                (image_id, original_path, storage_path, filename, file_size,
                 width, height, format, source_type, source_id, captured_at,
                 ingested_at, quality_score, blur_score, brightness, contrast,
                 perceptual_hash, processed, source_lakefs_commit, source_lakefs_branch)
                VALUES {', '.join(values_list)}
            """

            cursor.execute(sql)
            self.last_flush = datetime.now()
            logger.info(f"Flushed {len(records)} records to Iceberg")
            return len(records)

        except Exception as e:
            logger.error(f"Failed to flush to Iceberg: {e}")
            # Put records back in buffer for retry
            with self.buffer_lock:
                self.buffer = records + self.buffer
            raise

    def check_exists(self, image_id: str) -> bool:
        """Check if image already exists in Iceberg (deduplication)."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) FROM {self.config.image_metadata_table}
                WHERE image_id = {self._escape_string(image_id)}
            """)
            result = cursor.fetchone()
            return result and result[0] > 0
        except Exception:
            return False


# ===============================================================================
# KAFKA PRODUCER (for DLQ and Validated topics)
# ===============================================================================

class KafkaProducer:
    """Kafka producer for DLQ and validated messages."""

    def __init__(self, config: Config):
        self.config = config
        self._producer = None

    @property
    def producer(self):
        if self._producer is None:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": self.config.kafka_bootstrap_servers,
                "acks": "all",
            })
        return self._producer

    def send_to_dlq(
        self,
        original_message: Dict[str, Any],
        error_type: str,
        error_message: str,
        stack_trace: str = None,
        retry_count: int = 0,
        partition: int = None,
        offset: int = None,
    ):
        """Send failed message to DLQ."""
        dlq_message = {
            "original_message": original_message,
            "error": {
                "type": error_type,
                "message": error_message,
                "stack_trace": stack_trace,
            },
            "failed_at": datetime.utcnow().isoformat() + "Z",
            "retry_count": retry_count,
            "consumer_id": self.config.consumer_id,
            "topic": self.config.kafka_ingest_topic,
            "partition": partition,
            "offset": offset,
        }

        image_id = original_message.get("image_id", "unknown")
        self.producer.produce(
            self.config.kafka_dlq_topic,
            key=image_id.encode(),
            value=json.dumps(dlq_message).encode(),
        )
        self.producer.poll(0)

    def send_validated(
        self,
        image_id: str,
        storage_path: str,
        lakefs_commit: str,
        lakefs_branch: str,
        quality_score: float = None,
        perceptual_hash: str = None,
    ):
        """Send validated message to downstream topic."""
        message = {
            "image_id": image_id,
            "storage_path": storage_path,
            "lakefs_commit": lakefs_commit,
            "lakefs_branch": lakefs_branch,
            "validated_at": datetime.utcnow().isoformat() + "Z",
            "quality_score": quality_score,
            "perceptual_hash": perceptual_hash,
            "ready_for_detection": True,
        }

        self.producer.produce(
            self.config.kafka_validated_topic,
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        self.producer.poll(0)

    def flush(self, timeout: float = 10.0):
        """Flush pending messages."""
        self.producer.flush(timeout)


# ===============================================================================
# MESSAGE PROCESSOR
# ===============================================================================

class MessageProcessor:
    """Process ingestion messages."""

    SUPPORTED_FORMATS = {"jpg", "jpeg", "png", "webp"}

    def __init__(self, config: Config):
        self.config = config
        self.lakefs = LakeFSClient(config)
        self.minio = MinIOClient(config)
        self.iceberg = IcebergSink(config)
        self.kafka_producer = KafkaProducer(config)
        self.stats = defaultdict(int)
        self.pending_commits: List[Dict[str, Any]] = []

    def validate_message(self, message: Dict[str, Any]) -> tuple[bool, str]:
        """Validate message schema."""
        required_fields = ["image_id", "storage_path", "source_type", "timestamp"]

        for field in required_fields:
            if field not in message:
                return False, f"Missing required field: {field}"

        # Validate source_type
        valid_source_types = {"ip_camera", "edge_device", "batch_scan", "api_upload"}
        if message.get("source_type") not in valid_source_types:
            return False, f"Invalid source_type: {message.get('source_type')}"

        # Validate storage_path format
        storage_path = message.get("storage_path", "")
        if not storage_path.startswith(("s3://", "lakefs://", "minio://")):
            return False, f"Invalid storage_path format: {storage_path}"

        return True, ""

    def parse_storage_path(self, storage_path: str) -> tuple[str, str, str]:
        """
        Parse storage path into protocol, bucket, and key.

        Examples:
            s3://cv-raw/camera-01/2025/12/17/img.jpg -> (s3, cv-raw, camera-01/2025/12/17/img.jpg)
            lakefs://cv-data/dev/raw/img.jpg -> (lakefs, cv-data, dev/raw/img.jpg)
        """
        if storage_path.startswith("s3://"):
            path = storage_path[5:]
            parts = path.split("/", 1)
            return "s3", parts[0], parts[1] if len(parts) > 1 else ""
        elif storage_path.startswith("minio://"):
            path = storage_path[8:]
            parts = path.split("/", 1)
            return "minio", parts[0], parts[1] if len(parts) > 1 else ""
        elif storage_path.startswith("lakefs://"):
            path = storage_path[9:]
            parts = path.split("/", 2)
            return "lakefs", parts[0], "/".join(parts[1:]) if len(parts) > 1 else ""
        else:
            return "unknown", "", storage_path

    def verify_image_exists(self, storage_path: str) -> bool:
        """Verify image exists in storage."""
        protocol, bucket, key = self.parse_storage_path(storage_path)

        if protocol in ("s3", "minio"):
            return self.minio.object_exists(bucket, key)
        elif protocol == "lakefs":
            # For LakeFS paths, extract branch and path
            parts = key.split("/", 1)
            if len(parts) == 2:
                return self.lakefs.object_exists(parts[1])
        return False

    def get_image_data(self, storage_path: str) -> Optional[bytes]:
        """Get image data from storage."""
        protocol, bucket, key = self.parse_storage_path(storage_path)

        if protocol in ("s3", "minio"):
            return self.minio.get_object(bucket, key)
        return None

    def copy_to_lakefs(self, storage_path: str, image_id: str, metadata: Dict[str, Any]) -> Optional[str]:
        """Copy image from external storage to LakeFS."""
        # Get image data
        image_data = self.get_image_data(storage_path)
        if not image_data:
            return None

        # Determine file extension
        original_path = storage_path
        ext = original_path.rsplit(".", 1)[-1].lower() if "." in original_path else "jpg"

        # Build LakeFS path
        lakefs_path = f"{self.config.lakefs_raw_prefix}{image_id}.{ext}"

        # Upload to LakeFS
        content_type = f"image/{ext}" if ext in self.SUPPORTED_FORMATS else "application/octet-stream"
        if self.lakefs.upload_object(lakefs_path, image_data, content_type):
            return f"lakefs://{self.config.lakefs_repo}/{self.config.lakefs_branch}/{lakefs_path}"
        return None

    def process_message(
        self,
        message: Dict[str, Any],
        partition: int = None,
        offset: int = None,
    ) -> bool:
        """
        Process a single ingestion message.

        Returns True if successful, False if failed.
        """
        image_id = message.get("image_id", "unknown")

        try:
            # 1. Validate message schema
            valid, error = self.validate_message(message)
            if not valid:
                self.kafka_producer.send_to_dlq(
                    message, "ValidationError", error, partition=partition, offset=offset
                )
                self.stats["validation_errors"] += 1
                return False

            # 2. Check for duplicates (idempotency)
            if self.iceberg.check_exists(image_id):
                logger.debug(f"Skipping duplicate: {image_id}")
                self.stats["duplicates"] += 1
                return True  # Consider success - already processed

            # 3. Verify image exists in storage
            storage_path = message["storage_path"]
            if not self.verify_image_exists(storage_path):
                self.kafka_producer.send_to_dlq(
                    message, "StorageError", f"Image not found: {storage_path}",
                    partition=partition, offset=offset
                )
                self.stats["storage_errors"] += 1
                return False

            # 4. Copy to LakeFS if needed
            final_storage_path = storage_path
            if self.config.copy_to_lakefs and not storage_path.startswith("lakefs://"):
                lakefs_path = self.copy_to_lakefs(storage_path, image_id, message.get("metadata", {}))
                if lakefs_path:
                    final_storage_path = lakefs_path
                else:
                    logger.warning(f"Failed to copy to LakeFS, using original path: {storage_path}")

            # 5. Extract metadata
            metadata = message.get("metadata", {})
            filename = storage_path.rsplit("/", 1)[-1] if "/" in storage_path else image_id
            ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "unknown"

            # 6. Build Iceberg record
            record = {
                "image_id": image_id,
                "original_path": storage_path,
                "storage_path": final_storage_path,
                "filename": filename,
                "file_size": metadata.get("file_size_bytes", 0),
                "width": metadata.get("width", 0),
                "height": metadata.get("height", 0),
                "format": ext if ext in self.SUPPORTED_FORMATS else "unknown",
                "source_type": message["source_type"],
                "source_id": message.get("source_id", "unknown"),
                "captured_at": metadata.get("captured_at") or message["timestamp"],
                "ingested_at": datetime.utcnow().isoformat(),
                "quality_score": metadata.get("quality_score", 0.0),
                "blur_score": metadata.get("blur_score", 0.0),
                "brightness": metadata.get("brightness", 0.0),
                "contrast": metadata.get("contrast", 0.0),
                "perceptual_hash": metadata.get("perceptual_hash"),
                "source_lakefs_branch": self.config.lakefs_branch,
                "source_lakefs_commit": None,  # Will be set after commit
            }

            # 7. Add to Iceberg buffer
            self.iceberg.add(record)
            self.pending_commits.append({
                "image_id": image_id,
                "storage_path": final_storage_path,
                "quality_score": record["quality_score"],
                "perceptual_hash": record["perceptual_hash"],
            })

            self.stats["processed"] += 1
            return True

        except Exception as e:
            logger.error(f"Error processing message {image_id}: {e}")
            self.kafka_producer.send_to_dlq(
                message,
                type(e).__name__,
                str(e),
                stack_trace=traceback.format_exc(),
                partition=partition,
                offset=offset,
            )
            self.stats["errors"] += 1
            return False

    def commit_batch(self) -> Optional[str]:
        """Commit pending changes to LakeFS and flush to Iceberg."""
        if not self.pending_commits:
            return None

        # 1. Check for uncommitted LakeFS changes
        uncommitted = self.lakefs.get_uncommitted_changes()
        commit_id = None

        if uncommitted > 0:
            # 2. Commit to LakeFS
            commit_message = f"CVOps ingestion: {len(self.pending_commits)} images"
            commit_id = self.lakefs.commit(
                commit_message,
                metadata={
                    "source": "cv_ingest_consumer",
                    "consumer_id": self.config.consumer_id,
                    "image_count": str(len(self.pending_commits)),
                }
            )

            if commit_id:
                logger.info(f"LakeFS commit: {commit_id}")
            else:
                logger.warning("Failed to create LakeFS commit")

        # 3. Flush to Iceberg
        flushed = self.iceberg.flush()

        # 4. Send validated messages
        for item in self.pending_commits:
            self.kafka_producer.send_validated(
                image_id=item["image_id"],
                storage_path=item["storage_path"],
                lakefs_commit=commit_id or "no-commit",
                lakefs_branch=self.config.lakefs_branch,
                quality_score=item.get("quality_score"),
                perceptual_hash=item.get("perceptual_hash"),
            )

        self.kafka_producer.flush()
        self.pending_commits.clear()

        return commit_id


# ===============================================================================
# CONSUMER
# ===============================================================================

class CVIngestConsumer:
    """Main consumer class for CVOps image ingestion."""

    def __init__(self, config: Config):
        self.config = config
        self.processor = MessageProcessor(config)
        self.running = False
        self._consumer = None

    def _get_consumer(self):
        if self._consumer is None:
            from kafka import KafkaConsumer
            self._consumer = KafkaConsumer(
                self.config.kafka_ingest_topic,
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                group_id=self.config.kafka_consumer_group,
                auto_offset_reset=self.config.kafka_auto_offset_reset,
                enable_auto_commit=False,
                max_poll_records=self.config.kafka_max_poll_records,
                session_timeout_ms=self.config.kafka_session_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            )
        return self._consumer

    def run(self):
        """Main consumer loop."""
        self.running = True
        consumer = self._get_consumer()

        logger.info("=" * 60)
        logger.info("CVOps Kafka Ingestion Consumer Started")
        logger.info(f"  Consumer ID: {self.config.consumer_id}")
        logger.info(f"  Topic: {self.config.kafka_ingest_topic}")
        logger.info(f"  Group: {self.config.kafka_consumer_group}")
        logger.info(f"  LakeFS: {self.config.lakefs_repo}/{self.config.lakefs_branch}")
        logger.info(f"  Batch size: {self.config.batch_size}")
        logger.info("=" * 60)

        last_commit_time = time.time()
        message_count = 0

        try:
            while self.running:
                # Poll for messages
                records = consumer.poll(timeout_ms=1000)

                for tp, messages in records.items():
                    for message in messages:
                        if not self.running:
                            break

                        if message.value:
                            self.processor.process_message(
                                message.value,
                                partition=message.partition,
                                offset=message.offset,
                            )
                            message_count += 1

                # Check if we should commit
                should_commit = (
                    len(self.processor.pending_commits) >= self.config.batch_size or
                    (time.time() - last_commit_time) >= self.config.commit_interval_seconds
                )

                if should_commit and self.processor.pending_commits:
                    self.processor.commit_batch()
                    consumer.commit()
                    last_commit_time = time.time()

                    # Log stats periodically
                    stats = self.processor.stats
                    logger.info(
                        f"Stats: processed={stats['processed']}, "
                        f"duplicates={stats['duplicates']}, "
                        f"errors={stats['errors']}"
                    )

        except KeyboardInterrupt:
            logger.info("Shutdown requested...")
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown."""
        self.running = False
        logger.info("Shutting down...")

        # Commit any remaining messages
        if self.processor.pending_commits:
            logger.info(f"Committing {len(self.processor.pending_commits)} pending messages...")
            self.processor.commit_batch()

        if self._consumer:
            try:
                self._consumer.commit()
                self._consumer.close()
            except Exception as e:
                logger.warning(f"Error closing consumer: {e}")

        logger.info("Shutdown complete")
        self._log_final_stats()

    def _log_final_stats(self):
        """Log final statistics."""
        stats = self.processor.stats
        logger.info("=" * 60)
        logger.info("Final Statistics:")
        logger.info(f"  Total processed: {stats['processed']}")
        logger.info(f"  Duplicates skipped: {stats['duplicates']}")
        logger.info(f"  Validation errors: {stats['validation_errors']}")
        logger.info(f"  Storage errors: {stats['storage_errors']}")
        logger.info(f"  Other errors: {stats['errors']}")
        logger.info("=" * 60)


# ===============================================================================
# CLI
# ===============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="CVOps Kafka Ingestion Consumer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--kafka-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("CVOPS_KAFKA_INGEST_TOPIC", "cv.images.ingest"),
        help="Kafka topic to consume",
    )
    parser.add_argument(
        "--group",
        default=os.getenv("CVOPS_KAFKA_CONSUMER_GROUP", "cvops-ingest-consumer"),
        help="Kafka consumer group",
    )
    parser.add_argument(
        "--lakefs-server",
        default=os.getenv("LAKEFS_SERVER", "http://exp-lakefs:8000"),
        help="LakeFS server URL",
    )
    parser.add_argument(
        "--repo",
        default=os.getenv("CVOPS_REPO", "cv-data"),
        help="LakeFS repository",
    )
    parser.add_argument(
        "--branch",
        default=os.getenv("CVOPS_BRANCH", "dev-cvops"),
        help="LakeFS branch",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.getenv("CVOPS_KAFKA_BATCH_SIZE", "100")),
        help="Iceberg insert batch size",
    )
    parser.add_argument(
        "--no-lakefs-copy",
        action="store_true",
        help="Skip copying images to LakeFS",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Build config from args
    config = Config(
        kafka_bootstrap_servers=args.kafka_servers,
        kafka_ingest_topic=args.topic,
        kafka_consumer_group=args.group,
        lakefs_server=args.lakefs_server,
        lakefs_repo=args.repo,
        lakefs_branch=args.branch,
        batch_size=args.batch_size,
        copy_to_lakefs=not args.no_lakefs_copy,
        verbose=args.verbose,
    )

    if config.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Setup signal handlers
    consumer = CVIngestConsumer(config)

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        consumer.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run consumer
    consumer.run()


if __name__ == "__main__":
    main()
