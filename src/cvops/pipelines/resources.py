"""
CVOps Resources
===============

CV-specific resources for computer vision operations.
Extends the core resource pattern with:
- YOLOModelResource: Object detection model management
- CVImageStorageResource: Image storage with preprocessing
- CVKafkaResource: CV-specific Kafka topics
- CVIcebergResource: CV metadata tables

Design Principles:
1. Follows same patterns as core resources.py
2. Uses shared infrastructure (MinIO, Kafka, Trino)
3. Adds CV-specific functionality
"""
import os
import time
import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from io import BytesIO

from dagster import ConfigurableResource


# =============================================================================
# CONFIGURATION IMPORT
# =============================================================================

def get_trino_catalog():
    """Get TRINO_CATALOG from config."""
    try:
        from src.core.config import TRINO_CATALOG
        return TRINO_CATALOG
    except ImportError:
        return os.getenv("TRINO_CATALOG", "iceberg_dev")


def get_cvops_config():
    """Get CVOps configuration from Pydantic settings."""
    try:
        from src.core.config import settings
        # Return a dict-like interface for backward compatibility
        return {
            "repo": settings.cvops.repo,
            "branch": settings.cvops.branch,
            "raw_bucket": settings.cvops.raw_bucket,
            "yolo_model_path": settings.cvops.yolo_model_path,
            "yolo_confidence": settings.cvops.yolo_confidence,
            "yolo_device": settings.cvops.yolo_device,
            "yolo_dummy_mode": settings.cvops.yolo_dummy_mode,
        }
    except ImportError:
        # Fallback defaults
        return {
            "repo": os.getenv("CVOPS_LAKEFS_REPO", "cv-data"),
            "branch": os.getenv("CVOPS_DEV_BRANCH", "dev-cvops"),
            "raw_bucket": os.getenv("CVOPS_RAW_BUCKET", "cv-raw-images"),
            "yolo_model_path": os.getenv("YOLO_MODEL_PATH", "yolov8n.pt"),
            "yolo_confidence": float(os.getenv("YOLO_CONFIDENCE", "0.25")),
            "yolo_device": os.getenv("YOLO_DEVICE", "cpu"),
            "yolo_dummy_mode": os.getenv("YOLO_DUMMY_MODE", "").lower() in ("true", "1", "yes"),
        }


# =============================================================================
# IMAGE PROCESSING UTILITIES
# =============================================================================

def compute_image_quality(image) -> Dict[str, float]:
    """
    Compute image quality metrics.
    
    Returns:
        dict with quality_score (blur detection), brightness, contrast
    """
    import cv2
    import numpy as np
    
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # Blur detection using Laplacian variance
    quality_score = float(cv2.Laplacian(gray, cv2.CV_64F).var())
    
    # Brightness (mean pixel value)
    brightness = float(np.mean(gray))
    
    # Contrast (standard deviation)
    contrast = float(np.std(gray))
    
    return {
        "quality_score": quality_score,
        "brightness": brightness,
        "contrast": contrast,
    }


def compute_perceptual_hash(image) -> str:
    """
    Compute perceptual hash for deduplication.
    Uses dHash (difference hash) for robustness to minor changes.
    """
    import cv2
    
    # Resize to 9x8 for dHash
    resized = cv2.resize(image, (9, 8))
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    
    # Compute differences
    diff = gray[:, 1:] > gray[:, :-1]
    
    # Convert to hash
    return hashlib.md5(diff.tobytes()).hexdigest()


def preprocess_image(
    image,
    target_size: Tuple[int, int] = (640, 640),
) -> Tuple[Any, Dict[str, Any]]:
    """
    Preprocess image to target size with quality metrics.
    
    Returns:
        (preprocessed_image, metadata_dict)
    """
    import cv2
    
    original_h, original_w = image.shape[:2]
    
    # Resize maintaining aspect ratio with padding
    scale = min(target_size[0] / original_w, target_size[1] / original_h)
    new_w = int(original_w * scale)
    new_h = int(original_h * scale)
    
    resized = cv2.resize(image, (new_w, new_h), interpolation=cv2.INTER_LINEAR)
    
    # Pad to target size
    pad_w = (target_size[0] - new_w) // 2
    pad_h = (target_size[1] - new_h) // 2
    
    preprocessed = cv2.copyMakeBorder(
        resized,
        pad_h, target_size[1] - new_h - pad_h,
        pad_w, target_size[0] - new_w - pad_w,
        cv2.BORDER_CONSTANT,
        value=(114, 114, 114),  # YOLO gray padding
    )
    
    # Compute quality metrics on preprocessed image
    quality = compute_image_quality(preprocessed)
    
    metadata = {
        "original_width": original_w,
        "original_height": original_h,
        "preprocessed_width": target_size[0],
        "preprocessed_height": target_size[1],
        "scale_factor": scale,
        "pad_x": pad_w,
        "pad_y": pad_h,
        **quality,
    }
    
    return preprocessed, metadata


# =============================================================================
# YOLO MODEL RESOURCE
# =============================================================================

class YOLOModelResource(ConfigurableResource):
    """
    YOLO object detection model resource.
    
    Features:
    - Model loading from local path or MLflow
    - Caching for efficient reuse
    - Batch detection support
    - Dummy mode for testing without GPU
    """
    
    model_path: str = os.getenv("YOLO_MODEL_PATH", "yolov8n.pt")
    confidence: float = float(os.getenv("YOLO_CONFIDENCE", "0.25"))
    device: str = os.getenv("YOLO_DEVICE", "cpu")
    batch_size: int = int(os.getenv("YOLO_BATCH_SIZE", "8"))
    dummy_mode: bool = os.getenv("YOLO_DUMMY_MODE", "").lower() in ("true", "1", "yes")
    
    mlflow_tracking_uri: str = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")
    model_name: str = os.getenv("CVOPS_MLFLOW_MODEL", "yolo-object-detector")
    
    _model_cache: Dict[str, Any] = {}
    
    def load_model(
        self,
        model_stage: str = None,
        run_id: str = None,
    ) -> Tuple[Any, Dict[str, str]]:
        """
        Load YOLO model from MLflow or local path.
        
        Returns:
            (model, model_info_dict)
        """
        if self.dummy_mode:
            return None, {"source": "dummy", "device": "cpu"}
        
        # Build cache key
        if run_id:
            cache_key = f"run_{run_id}"
        elif model_stage:
            cache_key = f"stage_{model_stage}"
        else:
            cache_key = "local"
        
        if cache_key in self._model_cache:
            return self._model_cache[cache_key]
        
        model_info = {
            "model_name": self.model_name,
            "device": self.device,
        }
        
        # Try MLflow first
        if run_id or model_stage:
            try:
                import mlflow
                mlflow.set_tracking_uri(self.mlflow_tracking_uri)
                client = mlflow.tracking.MlflowClient()
                
                if run_id:
                    artifact_path = mlflow.artifacts.download_artifacts(
                        run_id=run_id,
                        artifact_path="model/best.pt",
                    )
                    model_info["run_id"] = run_id
                    model_info["source"] = "mlflow_run"
                else:
                    versions = client.get_latest_versions(self.model_name, stages=[model_stage])
                    if versions:
                        run_id = versions[0].run_id
                        artifact_path = mlflow.artifacts.download_artifacts(
                            run_id=run_id,
                            artifact_path="model/best.pt",
                        )
                        model_info["run_id"] = run_id
                        model_info["version"] = versions[0].version
                        model_info["stage"] = model_stage
                        model_info["source"] = "mlflow_registry"
                    else:
                        raise ValueError(f"No model in stage {model_stage}")
                
                from ultralytics import YOLO
                model = YOLO(artifact_path)
                model.to(self.device)
                
            except Exception as e:
                print(f"[YOLO] MLflow load failed: {e}, falling back to local")
                from ultralytics import YOLO
                model = YOLO(self.model_path)
                model.to(self.device)
                model_info["source"] = "local"
                model_info["path"] = self.model_path
        else:
            # Load local model
            from ultralytics import YOLO
            model = YOLO(self.model_path)
            model.to(self.device)
            model_info["source"] = "local"
            model_info["path"] = self.model_path
        
        self._model_cache[cache_key] = (model, model_info)
        return model, model_info
    
    def detect(
        self,
        image,
        model_stage: str = None,
        run_id: str = None,
    ) -> Tuple[List[Dict], float, Dict]:
        """
        Run detection on single image.
        
        Returns:
            (detections_list, inference_ms, model_info)
        """
        if self.dummy_mode:
            return self._dummy_detect(image)
        
        model, model_info = self.load_model(model_stage, run_id)
        
        start = time.time()
        results = model(image, conf=self.confidence, verbose=False)[0]
        inference_ms = (time.time() - start) * 1000
        
        detections = []
        img_h, img_w = image.shape[:2]
        
        for box in results.boxes:
            class_id = int(box.cls[0])
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            
            detections.append({
                "class_id": class_id,
                "class_name": model.names[class_id],
                "confidence": float(box.conf[0]),
                "bbox_x": x1 / img_w,
                "bbox_y": y1 / img_h,
                "bbox_width": (x2 - x1) / img_w,
                "bbox_height": (y2 - y1) / img_h,
            })
        
        return detections, inference_ms, model_info
    
    def detect_batch(
        self,
        images: List,
        model_stage: str = None,
        run_id: str = None,
    ) -> List[Tuple[List[Dict], float]]:
        """
        Run detection on batch of images.
        
        Returns:
            List of (detections_list, inference_ms) per image
        """
        if self.dummy_mode:
            return [self._dummy_detect(img)[:2] for img in images]
        
        model, model_info = self.load_model(model_stage, run_id)
        all_results = []
        
        for i in range(0, len(images), self.batch_size):
            batch = images[i:i + self.batch_size]
            
            start = time.time()
            results = model(batch, conf=self.confidence, verbose=False)
            batch_ms = (time.time() - start) * 1000
            per_image_ms = batch_ms / len(batch)
            
            for img, result in zip(batch, results):
                img_h, img_w = img.shape[:2]
                detections = []
                
                for box in result.boxes:
                    class_id = int(box.cls[0])
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    
                    detections.append({
                        "class_id": class_id,
                        "class_name": model.names[class_id],
                        "confidence": float(box.conf[0]),
                        "bbox_x": x1 / img_w,
                        "bbox_y": y1 / img_h,
                        "bbox_width": (x2 - x1) / img_w,
                        "bbox_height": (y2 - y1) / img_h,
                    })
                
                all_results.append((detections, per_image_ms))
        
        return all_results
    
    def _dummy_detect(self, image) -> Tuple[List[Dict], float, Dict]:
        """Return fake detections for testing."""
        detections = [
            {
                "class_id": 0,
                "class_name": "person",
                "confidence": 0.85,
                "bbox_x": 0.1,
                "bbox_y": 0.2,
                "bbox_width": 0.3,
                "bbox_height": 0.5,
            }
        ]
        return detections, 10.0, {"source": "dummy"}


# =============================================================================
# CV IMAGE STORAGE RESOURCE
# =============================================================================

class CVImageStorageResource(ConfigurableResource):
    """
    MinIO-based image storage with organized path structure.
    
    Storage Structure:
        cv-raw-images/
            {source_type}/{source_id}/{YYYY}/{MM}/{DD}/{image_id}.{ext}
        cv-preprocessed/
            640x640/{image_id}.png
    """
    
    endpoint: str = os.getenv("MINIO_ENDPOINT", "http://exp-minio:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "admin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "password123")
    
    raw_bucket: str = os.getenv("CVOPS_RAW_BUCKET", "cv-raw-images")
    preprocessed_bucket: str = os.getenv("CVOPS_PREPROCESSED_BUCKET", "cv-preprocessed")
    
    def _get_client(self):
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )
    
    def ensure_buckets(self):
        """Create buckets if they don't exist."""
        client = self._get_client()
        
        for bucket in [self.raw_bucket, self.preprocessed_bucket]:
            try:
                client.head_bucket(Bucket=bucket)
            except Exception:
                try:
                    client.create_bucket(Bucket=bucket)
                except Exception as e:
                    if "BucketAlreadyOwnedByYou" not in str(e):
                        raise
    
    def upload_raw_image(
        self,
        image_bytes: bytes,
        image_id: str,
        source_type: str,
        source_id: str,
        extension: str = "png",
        timestamp: datetime = None,
    ) -> str:
        """
        Upload raw image to MinIO with organized path structure.
        
        Returns:
            S3 path of uploaded image
        """
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        date_path = timestamp.strftime("%Y/%m/%d")
        key = f"{source_type}/{source_id}/{date_path}/{image_id}.{extension}"
        
        client = self._get_client()
        client.put_object(
            Bucket=self.raw_bucket,
            Key=key,
            Body=image_bytes,
            ContentType=f"image/{extension}",
        )
        
        return f"s3://{self.raw_bucket}/{key}"
    
    def upload_preprocessed_image(
        self,
        image_bytes: bytes,
        image_id: str,
        size: str = "640x640",
    ) -> str:
        """Upload preprocessed image."""
        key = f"{size}/{image_id}.png"
        
        client = self._get_client()
        client.put_object(
            Bucket=self.preprocessed_bucket,
            Key=key,
            Body=image_bytes,
            ContentType="image/png",
        )
        
        return f"s3://{self.preprocessed_bucket}/{key}"
    
    def download_image(self, bucket: str, key: str) -> bytes:
        """Download image from MinIO."""
        client = self._get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    
    def list_images(self, bucket: str, prefix: str = "", max_keys: int = 1000) -> List[str]:
        """List image keys in bucket."""
        client = self._get_client()
        
        keys = []
        continuation_token = None
        
        while len(keys) < max_keys:
            kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, max_keys - len(keys))}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            
            response = client.list_objects_v2(**kwargs)
            
            for obj in response.get("Contents", []):
                keys.append(obj["Key"])
            
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
        
        return keys


# =============================================================================
# CV ICEBERG RESOURCE
# =============================================================================

class CVIcebergResource(ConfigurableResource):
    """
    Iceberg tables for CV metadata via Trino.
    
    Tables:
        cv.image_metadata - Image catalog
        cv.detection_results - Detection outputs
        cv.annotations - Human annotations
        cv.model_metrics - Training metrics
    """
    
    host: str = os.getenv("TRINO_HOST", "exp-trino")
    port: int = int(os.getenv("TRINO_PORT", "8080"))
    user: str = os.getenv("TRINO_USER", "trino")
    catalog: str = os.getenv("TRINO_CATALOG", "iceberg_dev")
    
    def _get_connection(self):
        import trino
        return trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )
    
    def execute_query(self, query: str) -> List[Any]:
        """Execute SELECT query."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        finally:
            conn.close()
    
    def execute_ddl(self, query: str):
        """Execute DDL/DML."""
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
        finally:
            conn.close()
    
    def ensure_cv_schema(self):
        """Create CV schema and tables."""
        catalog = get_trino_catalog()
        self.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {catalog}.cv")

        # Image metadata table
        self.execute_ddl(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.cv.image_metadata (
            image_id VARCHAR,
            source_type VARCHAR,
            source_id VARCHAR,
            original_path VARCHAR,
            preprocessed_path VARCHAR,
            original_width INT,
            original_height INT,
            preprocessed_width INT,
            preprocessed_height INT,
            file_size_bytes BIGINT,
            format VARCHAR,
            quality_score DOUBLE,
            brightness DOUBLE,
            contrast DOUBLE,
            perceptual_hash VARCHAR,
            capture_timestamp TIMESTAMP,
            ingest_timestamp TIMESTAMP,
            lakefs_commit VARCHAR,
            processed BOOLEAN DEFAULT FALSE
        )
        """)
        
        # Detection results table
        self.execute_ddl(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.cv.detection_results (
            detection_id VARCHAR,
            image_id VARCHAR,
            model_name VARCHAR,
            model_version VARCHAR,
            run_id VARCHAR,
            class_name VARCHAR,
            class_id INT,
            confidence DOUBLE,
            bbox_x DOUBLE,
            bbox_y DOUBLE,
            bbox_width DOUBLE,
            bbox_height DOUBLE,
            inference_latency_ms DOUBLE,
            detected_at TIMESTAMP
        )
        """)
        
        # Annotations table (human labels)
        self.execute_ddl(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.cv.annotations (
            annotation_id VARCHAR,
            image_id VARCHAR,
            class_name VARCHAR,
            class_id INT,
            bbox_x DOUBLE,
            bbox_y DOUBLE,
            bbox_width DOUBLE,
            bbox_height DOUBLE,
            source VARCHAR,
            annotator_id VARCHAR,
            created_at TIMESTAMP
        )
        """)

        # Model metrics table
        self.execute_ddl(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.cv.model_metrics (
            metric_id VARCHAR,
            model_name VARCHAR,
            model_version VARCHAR,
            run_id VARCHAR,
            metric_name VARCHAR,
            class_name VARCHAR,
            metric_value DOUBLE,
            dataset_version VARCHAR,
            dataset_split VARCHAR,
            num_images INT,
            lakefs_commit VARCHAR,
            computed_at TIMESTAMP
        )
        """)
    
    def insert_image_metadata(self, records: List[Dict]):
        """Insert image metadata records."""
        if not records:
            return

        catalog = get_trino_catalog()
        for record in records:
            capture_ts = record.get("capture_timestamp")
            if isinstance(capture_ts, datetime):
                capture_ts = capture_ts.strftime("%Y-%m-%d %H:%M:%S")

            ingest_ts = record.get("ingest_timestamp", datetime.utcnow())
            if isinstance(ingest_ts, datetime):
                ingest_ts = ingest_ts.strftime("%Y-%m-%d %H:%M:%S")

            sql = f"""
            INSERT INTO {catalog}.cv.image_metadata VALUES (
                '{record.get("image_id", str(uuid.uuid4()))}',
                '{record.get("source_type", "unknown")}',
                '{record.get("source_id", "unknown")}',
                '{record.get("original_path", "")}',
                '{record.get("preprocessed_path", "")}',
                {record.get("original_width", 0)},
                {record.get("original_height", 0)},
                {record.get("preprocessed_width", 640)},
                {record.get("preprocessed_height", 640)},
                {record.get("file_size_bytes", 0)},
                '{record.get("format", "unknown")}',
                {record.get("quality_score", 0.0)},
                {record.get("brightness", 0.0)},
                {record.get("contrast", 0.0)},
                '{record.get("perceptual_hash", "")}',
                {f"TIMESTAMP '{capture_ts}'" if capture_ts else "NULL"},
                TIMESTAMP '{ingest_ts}',
                '{record.get("lakefs_commit", "")}',
                FALSE
            )
            """
            self.execute_ddl(sql)
    
    def insert_detection_results(self, records: List[Dict]):
        """Insert detection result records."""
        if not records:
            return

        catalog = get_trino_catalog()
        for record in records:
            detected_at = record.get("detected_at", datetime.utcnow())
            if isinstance(detected_at, datetime):
                detected_at = detected_at.strftime("%Y-%m-%d %H:%M:%S")

            sql = f"""
            INSERT INTO {catalog}.cv.detection_results VALUES (
                '{record.get("detection_id", str(uuid.uuid4()))}',
                '{record.get("image_id", "")}',
                '{record.get("model_name", "")}',
                '{record.get("model_version", "")}',
                '{record.get("run_id", "")}',
                '{record.get("class_name", "")}',
                {record.get("class_id", 0)},
                {record.get("confidence", 0.0)},
                {record.get("bbox_x", 0.0)},
                {record.get("bbox_y", 0.0)},
                {record.get("bbox_width", 0.0)},
                {record.get("bbox_height", 0.0)},
                {record.get("inference_latency_ms", 0.0)},
                TIMESTAMP '{detected_at}'
            )
            """
            self.execute_ddl(sql)


# =============================================================================
# CV KAFKA RESOURCE
# =============================================================================

class CVKafkaResource(ConfigurableResource):
    """
    Kafka producer for CV events.
    
    Topics:
        cv.images.raw - Raw image metadata events
        cv.images.preprocessed - Preprocessed image events
        cv.detections.results - Detection result events
    """
    
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
    
    topic_raw: str = "cv.images.raw"
    topic_preprocessed: str = "cv.images.preprocessed"
    topic_detections: str = "cv.detections.results"
    
    _producer: Any = None
    
    def _get_producer(self):
        if self._producer is None:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
            })
        return self._producer
    
    def publish_image_metadata(
        self,
        image_id: str,
        metadata: Dict[str, Any],
        topic: str = None,
    ):
        """Publish image metadata event (NOT image bytes)."""
        import json
        
        producer = self._get_producer()
        topic = topic or self.topic_raw
        
        message = {
            "image_id": image_id,
            "event_type": "image_ingested",
            "timestamp": datetime.utcnow().isoformat(),
            **metadata,
        }
        
        producer.produce(
            topic,
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        producer.poll(0)
    
    def publish_detection_result(
        self,
        image_id: str,
        detections: List[Dict],
        model_info: Dict[str, str],
        inference_ms: float,
    ):
        """Publish detection result event."""
        import json
        
        producer = self._get_producer()
        
        message = {
            "image_id": image_id,
            "event_type": "detection_completed",
            "timestamp": datetime.utcnow().isoformat(),
            "detections": detections,
            "model_name": model_info.get("model_name", ""),
            "model_version": model_info.get("version", ""),
            "run_id": model_info.get("run_id", ""),
            "inference_latency_ms": inference_ms,
        }
        
        producer.produce(
            self.topic_detections,
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        producer.poll(0)
    
    def flush(self, timeout: float = 10.0):
        """Flush pending messages."""
        producer = self._get_producer()
        producer.flush(timeout)


class CVKafkaIngestResource(ConfigurableResource):
    """
    Extended Kafka resource for CVOps data ingestion.

    Supports both streaming and batch ingestion patterns:
    - Batch producer for external storage scans (Dagster assets)
    - Consumer integration for processing ingestion messages
    - Topic management utilities
    - Consumer lag monitoring for backpressure

    Topics:
        cv.images.ingest - Primary ingestion topic (streaming + batch)
        cv.images.dlq - Dead letter queue for failed messages
        cv.images.validated - Successfully processed images
    """

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")

    topic_ingest: str = os.getenv("CVOPS_KAFKA_INGEST_TOPIC", "cv.images.ingest")
    topic_dlq: str = os.getenv("CVOPS_KAFKA_DLQ_TOPIC", "cv.images.dlq")
    topic_validated: str = os.getenv("CVOPS_KAFKA_VALIDATED_TOPIC", "cv.images.validated")

    consumer_group: str = os.getenv("CVOPS_KAFKA_CONSUMER_GROUP", "cvops-ingest-consumer")
    batch_size: int = int(os.getenv("CVOPS_KAFKA_BATCH_SIZE", "100"))

    _producer: Any = None
    _admin_client: Any = None

    def _get_producer(self):
        """Get or create Kafka producer."""
        if self._producer is None:
            from confluent_kafka import Producer
            self._producer = Producer({
                "bootstrap.servers": self.bootstrap_servers,
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 1000,
            })
        return self._producer

    def _get_admin_client(self):
        """Get or create Kafka admin client."""
        if self._admin_client is None:
            from confluent_kafka.admin import AdminClient
            self._admin_client = AdminClient({
                "bootstrap.servers": self.bootstrap_servers,
            })
        return self._admin_client

    def produce_ingest_message(
        self,
        image_id: str,
        storage_path: str,
        source_type: str,
        source_id: str,
        metadata: Dict[str, Any] = None,
        batch_id: str = None,
        priority: str = "normal",
    ):
        """
        Produce a single image ingestion message.

        Args:
            image_id: Unique image identifier (UUID)
            storage_path: Full path to image in storage (s3://bucket/key or lakefs://repo/branch/path)
            source_type: Origin of the image (ip_camera, edge_device, batch_scan, api_upload)
            source_id: Identifier of the source (camera ID, bucket name, etc.)
            metadata: Optional metadata dict (width, height, format, file_size_bytes, etc.)
            batch_id: Batch identifier for batch ingestion (None for streaming)
            priority: Message priority (high, normal, low)
        """
        import json

        producer = self._get_producer()

        message = {
            "image_id": image_id,
            "storage_path": storage_path,
            "source_type": source_type,
            "source_id": source_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metadata": metadata or {},
            "batch_id": batch_id,
            "priority": priority,
        }

        producer.produce(
            self.topic_ingest,
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        producer.poll(0)

    def produce_batch(
        self,
        images: List[Dict[str, Any]],
        batch_id: str,
        source_type: str = "batch_scan",
        source_id: str = None,
    ) -> int:
        """
        Batch produce messages for storage scan results.

        Args:
            images: List of image dicts with keys: image_id, storage_path, metadata (optional)
            batch_id: Unique batch identifier
            source_type: Source type for all images in batch
            source_id: Source identifier (e.g., bucket name)

        Returns:
            Number of messages produced
        """
        import json

        producer = self._get_producer()
        produced = 0

        for img in images:
            message = {
                "image_id": img["image_id"],
                "storage_path": img["storage_path"],
                "source_type": source_type,
                "source_id": source_id or img.get("source_id", "unknown"),
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metadata": img.get("metadata", {}),
                "batch_id": batch_id,
                "priority": img.get("priority", "normal"),
            }

            producer.produce(
                self.topic_ingest,
                key=img["image_id"].encode(),
                value=json.dumps(message).encode(),
            )
            produced += 1

            # Poll periodically to handle delivery callbacks
            if produced % 100 == 0:
                producer.poll(0)

        # Flush remaining messages
        producer.flush(timeout=30.0)
        return produced

    def produce_to_dlq(
        self,
        original_message: Dict[str, Any],
        error_type: str,
        error_message: str,
        stack_trace: str = None,
        retry_count: int = 0,
        consumer_id: str = None,
        partition: int = None,
        offset: int = None,
    ):
        """
        Produce a failed message to the dead letter queue.

        Args:
            original_message: The original message that failed
            error_type: Type/class of the error
            error_message: Error description
            stack_trace: Optional stack trace
            retry_count: Number of retry attempts made
            consumer_id: ID of the consumer that processed the message
            partition: Source partition
            offset: Source offset
        """
        import json

        producer = self._get_producer()

        dlq_message = {
            "original_message": original_message,
            "error": {
                "type": error_type,
                "message": error_message,
                "stack_trace": stack_trace,
            },
            "failed_at": datetime.utcnow().isoformat() + "Z",
            "retry_count": retry_count,
            "consumer_id": consumer_id,
            "topic": self.topic_ingest,
            "partition": partition,
            "offset": offset,
        }

        image_id = original_message.get("image_id", "unknown")

        producer.produce(
            self.topic_dlq,
            key=image_id.encode(),
            value=json.dumps(dlq_message).encode(),
        )
        producer.poll(0)

    def produce_validated(
        self,
        image_id: str,
        storage_path: str,
        lakefs_commit: str,
        lakefs_branch: str,
        iceberg_record_id: str = None,
        quality_score: float = None,
        perceptual_hash: str = None,
        ready_for_detection: bool = True,
    ):
        """
        Produce a validated image message to downstream topic.

        Args:
            image_id: Unique image identifier
            storage_path: Path to image in LakeFS
            lakefs_commit: LakeFS commit ID
            lakefs_branch: LakeFS branch
            iceberg_record_id: Optional Iceberg record ID
            quality_score: Optional quality score
            perceptual_hash: Optional perceptual hash for deduplication
            ready_for_detection: Whether image is ready for detection pipeline
        """
        import json

        producer = self._get_producer()

        message = {
            "image_id": image_id,
            "storage_path": storage_path,
            "lakefs_commit": lakefs_commit,
            "lakefs_branch": lakefs_branch,
            "iceberg_record_id": iceberg_record_id,
            "validated_at": datetime.utcnow().isoformat() + "Z",
            "quality_score": quality_score,
            "perceptual_hash": perceptual_hash,
            "ready_for_detection": ready_for_detection,
        }

        producer.produce(
            self.topic_validated,
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        producer.poll(0)

    def get_consumer_lag(self, group_id: str = None) -> Dict[str, int]:
        """
        Get consumer lag for monitoring and backpressure.

        Args:
            group_id: Consumer group ID (defaults to self.consumer_group)

        Returns:
            Dict mapping partition to lag count
        """
        from confluent_kafka import Consumer, TopicPartition

        group_id = group_id or self.consumer_group

        # Create a temporary consumer to query offsets
        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        })

        try:
            # Get partition info
            metadata = consumer.list_topics(self.topic_ingest, timeout=10)
            topic_metadata = metadata.topics.get(self.topic_ingest)

            if not topic_metadata:
                return {}

            lag_info = {}

            for partition_id in topic_metadata.partitions:
                tp = TopicPartition(self.topic_ingest, partition_id)

                # Get committed offset
                committed = consumer.committed([tp], timeout=10)
                committed_offset = committed[0].offset if committed and committed[0].offset >= 0 else 0

                # Get high watermark (latest offset)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)

                lag_info[partition_id] = max(0, high - committed_offset)

            return lag_info
        finally:
            consumer.close()

    def get_total_lag(self, group_id: str = None) -> int:
        """Get total consumer lag across all partitions."""
        lag_info = self.get_consumer_lag(group_id)
        return sum(lag_info.values())

    def get_dlq_depth(self) -> int:
        """
        Get the number of messages in the DLQ.

        Returns:
            Total message count in DLQ
        """
        from confluent_kafka import Consumer

        consumer = Consumer({
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": f"{self.consumer_group}-dlq-monitor",
            "enable.auto.commit": False,
        })

        try:
            metadata = consumer.list_topics(self.topic_dlq, timeout=10)
            topic_metadata = metadata.topics.get(self.topic_dlq)

            if not topic_metadata:
                return 0

            total = 0
            for partition_id in topic_metadata.partitions:
                from confluent_kafka import TopicPartition
                tp = TopicPartition(self.topic_dlq, partition_id)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                total += (high - low)

            return total
        finally:
            consumer.close()

    def create_topics(self, num_partitions: int = 6, replication_factor: int = 1):
        """
        Create ingestion topics if they don't exist.

        Args:
            num_partitions: Number of partitions for main topics
            replication_factor: Replication factor (1 for single broker)
        """
        from confluent_kafka.admin import NewTopic

        admin = self._get_admin_client()

        topics_config = [
            NewTopic(
                self.topic_ingest,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config={"retention.ms": str(7 * 24 * 60 * 60 * 1000)}  # 7 days
            ),
            NewTopic(
                self.topic_dlq,
                num_partitions=3,
                replication_factor=replication_factor,
                config={"retention.ms": str(30 * 24 * 60 * 60 * 1000)}  # 30 days
            ),
            NewTopic(
                self.topic_validated,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config={"retention.ms": str(3 * 24 * 60 * 60 * 1000)}  # 3 days
            ),
        ]

        # Create topics (ignores already existing)
        futures = admin.create_topics(topics_config)

        results = {}
        for topic, future in futures.items():
            try:
                future.result()
                results[topic] = "created"
            except Exception as e:
                if "TopicExistsError" in str(type(e).__name__) or "TOPIC_ALREADY_EXISTS" in str(e):
                    results[topic] = "exists"
                else:
                    results[topic] = f"error: {e}"

        return results

    def flush(self, timeout: float = 10.0):
        """Flush pending producer messages."""
        if self._producer:
            self._producer.flush(timeout)


# =============================================================================
# EXPORTS
# =============================================================================

CVOPS_RESOURCES = {
    "cv_yolo": YOLOModelResource(),
    "cv_storage": CVImageStorageResource(),
    "cv_iceberg": CVIcebergResource(),
    "cv_kafka": CVKafkaResource(),
    "cv_kafka_ingest": CVKafkaIngestResource(),
}
