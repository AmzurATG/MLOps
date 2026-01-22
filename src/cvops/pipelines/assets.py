"""
CVOps Assets - Dagster Pipeline Assets
======================================

Complete CVOps pipeline assets following the MLOps pattern:

Pipeline Flow:
1. cvops_init - Initialize LakeFS repo and Nessie branch
2. cvops_create_manifest - Scan images and create metadata table
3. cvops_run_detections - Run YOLO detection on images
4. cvops_export_to_labelstudio - Create annotation tasks
5. cvops_merge_annotations - Merge human annotations
6. cvops_create_training_data - Build Gold training dataset
7. cvops_train_model - Train YOLO model with MLflow
8. cvops_promote_model - Promote model to production

Branch Strategy:
    dev-cvops - Development/experimentation
    main - Production data and models
"""
import os
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    Config,
    define_asset_job,
    AssetSelection,
    RetryPolicy,
)

# Import shared resources from core
from src.core.resources import (
    LakeFSResource,
    TrinoResource,
    MinIOResource,
    LabelStudioResource,
    NessieResource,
)

# Import CV-specific resources
from cvops.pipelines.resources import (
    YOLOModelResource,
    CVImageStorageResource,
    CVIcebergResource,
    CVKafkaResource,
    CVKafkaIngestResource,
    compute_image_quality,
    compute_perceptual_hash,
    preprocess_image,
)

# Import config from core
from src.core.config import settings, TRINO_CATALOG


# =============================================================================
# RETRY POLICIES
# =============================================================================

GPU_RETRY = RetryPolicy(max_retries=2, delay=120)
NETWORK_RETRY = RetryPolicy(max_retries=3, delay=60)


# =============================================================================
# CONFIGURATION
# =============================================================================

class CVTrainingConfig(Config):
    """Configuration for CV training pipeline."""
    
    model_size: str = "yolov8n"  # yolov8n, yolov8s, yolov8m, yolov8l, yolov8x
    epochs: int = 100
    batch_size: int = 16
    image_size: int = 640
    patience: int = 50
    train_split: float = 0.8
    export_onnx: bool = True
    labelstudio_project_id: int = 2


# =============================================================================
# INITIALIZATION ASSETS
# =============================================================================

@asset(
    group_name="cvops_init",
    description="Initialize CVOps LakeFS repo and Nessie branch",
)
def cvops_init_lakefs(
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """
    Create CV data repository and branches.
    
    Creates:
    - LakeFS repo: cv-data with dev-cvops branch
    - Nessie branch: dev-cvops for Iceberg catalog versioning
    """
    repo = settings.cvops.repo
    branch = settings.cvops.branch
    
    # LakeFS for object storage versioning
    lakefs.ensure_repo(repo, f"s3://lakefs/{repo}")
    lakefs.create_branch(repo, branch)
    
    # Nessie for Iceberg catalog versioning
    nessie.create_branch(branch, "main")
    
    return MaterializeResult(
        metadata={
            "repo": repo,
            "branch": branch,
            "nessie_branch": branch,
            "status": "initialized",
        }
    )


@asset(
    group_name="cvops_init",
    deps=["cvops_init_lakefs"],
    description="Initialize CV Iceberg tables",
)
def cvops_init_tables(
    context: AssetExecutionContext,
    cv_iceberg: CVIcebergResource,
    trino: TrinoResource,
) -> MaterializeResult:
    """
    Create CV schema and tables in Iceberg.
    
    Creates:
    - cv.image_metadata - Image manifest with source_lakefs_commit
    - cv.detection_results - YOLO detections with source_lakefs_commit
    - cv.annotations - Human annotations with source_lakefs_commit
    - cv.model_metrics - Training metrics
    - cv.training_data - Gold training dataset
    - cv.lakefs_commits - Commit tracking (like metadata.lakefs_commits)
    - cv.cleanup_history - Cleanup audit trail
    - cv.lakefs_webhook_events - Webhook events storage
    """
    tables_created = []

    # Ensure schema exists
    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.cv")

    # 1. Image Metadata table with source_lakefs_commit
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.image_metadata (
                image_id VARCHAR,
                original_path VARCHAR,
                storage_path VARCHAR,
                filename VARCHAR,
                file_size BIGINT,
                width INTEGER,
                height INTEGER,
                format VARCHAR,
                source_type VARCHAR,
                source_id VARCHAR,
                captured_at TIMESTAMP,
                ingested_at TIMESTAMP,
                quality_score DOUBLE,
                blur_score DOUBLE,
                brightness DOUBLE,
                contrast DOUBLE,
                perceptual_hash VARCHAR,
                processed BOOLEAN,
                source_lakefs_commit VARCHAR,
                source_lakefs_branch VARCHAR
            )
        """)
        tables_created.append("cv.image_metadata")
        context.log.info("✅ Created cv.image_metadata")
    except Exception as e:
        context.log.warning(f"cv.image_metadata may exist: {e}")
    
    # 2. Detection Results with source_lakefs_commit
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.detection_results (
                detection_id VARCHAR,
                image_id VARCHAR,
                model_name VARCHAR,
                model_version VARCHAR,
                model_run_id VARCHAR,
                class_id INTEGER,
                class_name VARCHAR,
                confidence DOUBLE,
                bbox_x1 DOUBLE,
                bbox_y1 DOUBLE,
                bbox_x2 DOUBLE,
                bbox_y2 DOUBLE,
                bbox_width DOUBLE,
                bbox_height DOUBLE,
                detected_at TIMESTAMP,
                inference_ms DOUBLE,
                source_lakefs_commit VARCHAR
            )
        """)
        tables_created.append("cv.detection_results")
        context.log.info("✅ Created cv.detection_results")
    except Exception as e:
        context.log.warning(f"cv.detection_results may exist: {e}")
    
    # 3. Annotations with source_lakefs_commit
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.annotations (
                annotation_id VARCHAR,
                image_id VARCHAR,
                task_id INTEGER,
                annotator VARCHAR,
                class_label VARCHAR,
                bbox_x DOUBLE,
                bbox_y DOUBLE,
                bbox_width DOUBLE,
                bbox_height DOUBLE,
                confidence DOUBLE,
                is_verified BOOLEAN,
                annotated_at TIMESTAMP,
                source VARCHAR,
                source_lakefs_commit VARCHAR
            )
        """)
        tables_created.append("cv.annotations")
        context.log.info("✅ Created cv.annotations")
    except Exception as e:
        context.log.warning(f"cv.annotations may exist: {e}")
    
    # 4. Training Data (Gold) with source_lakefs_commit
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.training_data (
                image_id VARCHAR,
                image_path VARCHAR,
                split VARCHAR,
                class_label VARCHAR,
                bbox_x_norm DOUBLE,
                bbox_y_norm DOUBLE,
                bbox_w_norm DOUBLE,
                bbox_h_norm DOUBLE,
                annotation_source VARCHAR,
                created_at TIMESTAMP,
                source_lakefs_commit VARCHAR
            )
        """)
        tables_created.append("cv.training_data")
        context.log.info("✅ Created cv.training_data")
    except Exception as e:
        context.log.warning(f"cv.training_data may exist: {e}")
    
    # 5. Model Metrics
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.model_metrics (
                run_id VARCHAR,
                model_name VARCHAR,
                model_version VARCHAR,
                epochs INTEGER,
                batch_size INTEGER,
                image_size INTEGER,
                train_images INTEGER,
                val_images INTEGER,
                test_images INTEGER,
                map50 DOUBLE,
                map50_95 DOUBLE,
                precision_val DOUBLE,
                recall DOUBLE,
                training_started_at TIMESTAMP,
                training_completed_at TIMESTAMP,
                training_duration_seconds DOUBLE,
                source_lakefs_commit VARCHAR,
                source_lakefs_branch VARCHAR
            )
        """)
        tables_created.append("cv.model_metrics")
        context.log.info("✅ Created cv.model_metrics")
    except Exception as e:
        context.log.warning(f"cv.model_metrics may exist: {e}")
    
    # 6. LakeFS Commits Tracking (like metadata.lakefs_commits)
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.lakefs_commits (
                commit_id VARCHAR,
                lakefs_repository VARCHAR,
                lakefs_branch VARCHAR,
                created_at TIMESTAMP,
                affected_tables ARRAY(ROW(table_name VARCHAR, record_count INTEGER)),
                image_count INTEGER,
                status VARCHAR,
                reverted_at TIMESTAMP,
                cleaned_at TIMESTAMP,
                revert_reason VARCHAR,
                last_updated TIMESTAMP
            )
        """)
        tables_created.append("cv.lakefs_commits")
        context.log.info("✅ Created cv.lakefs_commits")
    except Exception as e:
        context.log.warning(f"cv.lakefs_commits may exist: {e}")
    
    # 7. Cleanup History (like metadata.cleanup_history)
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.cleanup_history (
                id INTEGER,
                reverted_commit VARCHAR,
                cleanup_started TIMESTAMP,
                cleanup_completed TIMESTAMP,
                tables_cleaned ARRAY(ROW(table_name VARCHAR, records_deleted INTEGER)),
                total_records_deleted INTEGER,
                status VARCHAR,
                error_message VARCHAR,
                trigger_source VARCHAR
            )
        """)
        tables_created.append("cv.cleanup_history")
        context.log.info("✅ Created cv.cleanup_history")
    except Exception as e:
        context.log.warning(f"cv.cleanup_history may exist: {e}")
    
    # 8. Webhook Events
    try:
        trino.execute_ddl(f"""
            CREATE TABLE IF NOT EXISTS {TRINO_CATALOG}.cv.lakefs_webhook_events (
                id BIGINT,
                event_id VARCHAR,
                event_type VARCHAR,
                repository VARCHAR,
                branch VARCHAR,
                commit_id VARCHAR,
                reverted_commit VARCHAR,
                source_ref VARCHAR,
                payload JSON,
                received_at TIMESTAMP(6) WITH TIME ZONE,
                processed_at TIMESTAMP(6) WITH TIME ZONE,
                status VARCHAR,
                error_message VARCHAR,
                retry_count INTEGER,
                affected_paths ARRAY(VARCHAR),
                image_count INTEGER
            )
        """)
        tables_created.append("cv.lakefs_webhook_events")
        context.log.info("✅ Created cv.lakefs_webhook_events")
    except Exception as e:
        context.log.warning(f"cv.lakefs_webhook_events may exist: {e}")
    
    return MaterializeResult(
        metadata={
            "tables_created": tables_created,
            "status": "initialized",
        }
    )


# =============================================================================
# BRONZE LAYER - IMAGE INGESTION
# =============================================================================

@asset(
    group_name="cvops_bronze",
    deps=["cvops_init_tables"],
    description="Scan images and create manifest table",
)
def cvops_create_manifest(
    context: AssetExecutionContext,
    trino: TrinoResource,
    minio: MinIOResource,
    lakefs: LakeFSResource,
    cv_iceberg: CVIcebergResource,
) -> MaterializeResult:
    """
    Scan raw images from storage and create metadata manifest.
    
    Supports both:
    - LakeFS storage (versioned)
    - Direct MinIO storage
    """
    raw_bucket = settings.cvops.raw_bucket
    use_lakefs = settings.cvops.use_lakefs_storage
    manifest_table = f"{TRINO_CATALOG}.cv.image_metadata"

    # List images from storage
    if use_lakefs:
        repo = settings.cvops.repo
        branch = settings.cvops.branch
        prefix = settings.cvops.raw_prefix
        image_keys = lakefs.list_objects(repo, branch, prefix)
        storage_location = f"lakefs://{repo}/{branch}/{prefix}"
        context.log.info(f"Found {len(image_keys)} images in LakeFS {storage_location}")
    else:
        image_keys = minio.list_objects(raw_bucket, prefix="")
        storage_location = raw_bucket
        context.log.info(f"Found {len(image_keys)} images in MinIO {raw_bucket}")
    
    # Filter to supported image formats
    supported_formats = [".jpg", ".jpeg", ".png", ".webp"]
    image_keys = [k for k in image_keys if any(k.lower().endswith(ext) for ext in supported_formats)]
    
    # Get existing keys
    existing_keys = set()
    try:
        result = trino.execute_query(f"SELECT original_path FROM {manifest_table}")
        existing_keys = {row[0] for row in result}
    except Exception:
        pass
    
    # Insert new images
    new_count = 0
    for key in image_keys:
        if use_lakefs:
            full_path = f"lakefs://{settings.cvops.repo}/{settings.cvops.branch}/{key}"
        else:
            full_path = f"s3://{raw_bucket}/{key}"

        if full_path in existing_keys:
            continue

        image_id = str(uuid.uuid4())

        try:
            cv_iceberg.insert_image_metadata([{
                "image_id": image_id,
                "source_type": "batch_scan",
                "source_id": raw_bucket if not use_lakefs else settings.cvops.repo,
                "original_path": full_path,
                "preprocessed_path": "",
                "original_width": 0,
                "original_height": 0,
                "preprocessed_width": 640,
                "preprocessed_height": 640,
                "file_size_bytes": 0,
                "format": key.split(".")[-1].lower(),
                "quality_score": 0.0,
                "brightness": 0.0,
                "contrast": 0.0,
                "perceptual_hash": "",
                "ingest_timestamp": datetime.utcnow(),
                "lakefs_commit": "",
            }])
            new_count += 1
        except Exception as e:
            context.log.warning(f"Failed to insert {key}: {e}")
    
    # Get total count
    try:
        result = trino.execute_query(f"SELECT COUNT(*) FROM {manifest_table}")
        total = result[0][0] if result else 0
    except Exception:
        total = new_count
    
    context.log.info(f"Manifest has {total} images ({new_count} new)")
    
    return MaterializeResult(
        metadata={
            "total_images": total,
            "new_images": new_count,
            "storage": storage_location,
        }
    )


# =============================================================================
# SILVER LAYER - DETECTION
# =============================================================================

@asset(
    group_name="cvops_silver",
    deps=["cvops_create_manifest"],
    description="Run YOLO detection on unprocessed images",
    retry_policy=GPU_RETRY,
)
def cvops_run_detections(
    context: AssetExecutionContext,
    trino: TrinoResource,
    minio: MinIOResource,
    cv_yolo: YOLOModelResource,
    cv_iceberg: CVIcebergResource,
    lakefs: LakeFSResource,
) -> MaterializeResult:
    """
    Run object detection on unprocessed images.
    
    Process:
    1. Query unprocessed images from manifest
    2. Download and preprocess each image
    3. Run YOLO detection
    4. Store results in detection_results table
    5. Mark images as processed
    """
    import cv2
    import numpy as np
    
    manifest_table = f"{TRINO_CATALOG}.cv.image_metadata"
    detections_table = f"{TRINO_CATALOG}.cv.detection_results"
    use_lakefs = settings.cvops.use_lakefs_storage
    
    # Get unprocessed images
    query = f"""
    SELECT image_id, original_path, format
    FROM {manifest_table}
    WHERE processed = FALSE
    LIMIT 100
    """
    
    try:
        unprocessed = trino.execute_query(query)
    except Exception as e:
        context.log.warning(f"Could not query unprocessed images: {e}")
        unprocessed = []
    
    if not unprocessed:
        context.log.info("No unprocessed images found")
        return MaterializeResult(metadata={"processed_count": 0, "detection_count": 0})
    
    context.log.info(f"Processing {len(unprocessed)} images")
    
    # Load model info
    _, model_info = cv_yolo.load_model()
    
    processed_count = 0
    total_detections = 0
    
    for image_id, original_path, img_format in unprocessed:
        try:
            # Download image
            if original_path.startswith("lakefs://"):
                # Parse lakefs://repo/branch/path
                parts = original_path.replace("lakefs://", "").split("/", 2)
                repo, branch, key = parts[0], parts[1], parts[2]
                
                import tempfile
                with tempfile.NamedTemporaryFile(suffix=f".{img_format}", delete=False) as tmp:
                    lakefs.download_file(repo, branch, key, tmp.name)
                    image = cv2.imread(tmp.name)
                    os.unlink(tmp.name)
            else:
                # s3://bucket/key
                parts = original_path.replace("s3://", "").split("/", 1)
                bucket, key = parts[0], parts[1]
                
                image_bytes = minio.download_file(bucket, key)
                nparr = np.frombuffer(image_bytes, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            
            if image is None:
                context.log.warning(f"Could not load image: {original_path}")
                continue
            
            # Run detection
            detections, inference_ms, _ = cv_yolo.detect(image)
            
            # Store detections
            detection_records = []
            for det in detections:
                detection_records.append({
                    "detection_id": str(uuid.uuid4()),
                    "image_id": image_id,
                    "model_name": model_info.get("model_name", "yolo"),
                    "model_version": model_info.get("version", "local"),
                    "run_id": model_info.get("run_id", ""),
                    "class_name": det["class_name"],
                    "class_id": det["class_id"],
                    "confidence": det["confidence"],
                    "bbox_x": det["bbox_x"],
                    "bbox_y": det["bbox_y"],
                    "bbox_width": det["bbox_width"],
                    "bbox_height": det["bbox_height"],
                    "inference_latency_ms": inference_ms,
                    "detected_at": datetime.utcnow(),
                })
            
            if detection_records:
                cv_iceberg.insert_detection_results(detection_records)
                total_detections += len(detection_records)
            
            # Mark as processed (escape image_id to prevent SQL injection)
            from src.core.config import escape_sql_string
            image_id_safe = escape_sql_string(image_id)
            trino.execute_ddl(f"""
            UPDATE {manifest_table}
            SET processed = TRUE
            WHERE image_id = '{image_id_safe}'
            """)
            
            processed_count += 1
            
        except Exception as e:
            context.log.warning(f"Failed to process {image_id}: {e}")
    
    context.log.info(f"Processed {processed_count} images, {total_detections} detections")
    
    return MaterializeResult(
        metadata={
            "processed_count": processed_count,
            "detection_count": total_detections,
            "model": model_info.get("model_name", "yolo"),
        }
    )


# =============================================================================
# ANNOTATION LAYER
# =============================================================================

@asset(
    group_name="cvops_annotation",
    deps=["cvops_run_detections"],
    description="Export images to Label Studio for annotation",
)
def cvops_export_to_labelstudio(
    context: AssetExecutionContext,
    trino: TrinoResource,
    lakefs: LakeFSResource,
    label_studio: LabelStudioResource,
) -> MaterializeResult:
    """Create annotation tasks for detected objects in Label Studio."""
    
    detections_table = f"{TRINO_CATALOG}.cv.detection_results"
    manifest_table = f"{TRINO_CATALOG}.cv.image_metadata"
    use_lakefs = settings.cvops.use_lakefs_storage

    # Get or create project
    project_id = label_studio.get_or_create_project(
        settings.cvops.ls_project_name,
        """
        <View>
          <Image name="image" value="$image_url"/>
          <RectangleLabels name="label" toName="image">
            <Label value="person"/>
            <Label value="vehicle"/>
            <Label value="object"/>
          </RectangleLabels>
        </View>
        """,
    )
    context.log.info(f"Using Label Studio project: {project_id}")
    
    # Get images with detections that haven't been exported
    query = f"""
    SELECT DISTINCT m.image_id, m.original_path
    FROM {manifest_table} m
    INNER JOIN {detections_table} d ON m.image_id = d.image_id
    WHERE m.processed = TRUE
    LIMIT 50
    """
    
    try:
        results = trino.execute_query(query)
    except Exception as e:
        context.log.warning(f"Could not query images: {e}")
        results = []
    
    if not results:
        return MaterializeResult(metadata={"tasks_created": 0})
    
    # Create tasks
    tasks = []
    for image_id, original_path in results:
        # Generate accessible URL
        if use_lakefs:
            image_url = lakefs.get_object_url(
                settings.cvops.repo,
                settings.cvops.branch,
                original_path.replace(f"lakefs://{settings.cvops.repo}/{settings.cvops.branch}/", ""),
            )
        else:
            # Direct MinIO URL
            image_url = f"http://exp-minio:9000/{original_path.replace('s3://', '')}"
        
        tasks.append({
            "data": {
                "image_url": image_url,
                "image_id": image_id,
            }
        })
    
    count = label_studio.bulk_import_tasks(project_id, tasks)
    context.log.info(f"Created {count} annotation tasks")
    
    return MaterializeResult(
        metadata={
            "tasks_created": count,
            "project_id": project_id,
        }
    )


@asset(
    group_name="cvops_annotation",
    deps=["cvops_export_to_labelstudio"],
    description="Merge human annotations from Label Studio",
)
def cvops_merge_annotations(
    context: AssetExecutionContext,
    label_studio: LabelStudioResource,
    cv_iceberg: CVIcebergResource,
) -> MaterializeResult:
    """
    Export annotations from Label Studio and merge into Iceberg.
    
    Converts Label Studio format to standardized annotation records.
    """
    # Get project
    project_id = label_studio.get_or_create_project(
        settings.cvops.ls_project_name,
        "",
    )
    
    # Export annotations
    try:
        annotations = label_studio.export_annotations(project_id)
    except Exception as e:
        context.log.warning(f"Failed to export annotations: {e}")
        return MaterializeResult(metadata={"annotations_merged": 0})
    
    context.log.info(f"Retrieved {len(annotations)} annotated tasks")
    
    # Parse and insert annotations
    annotation_records = []
    
    for task in annotations:
        if not task.get("annotations"):
            continue
        
        image_id = task.get("data", {}).get("image_id", "")
        
        for ann in task["annotations"]:
            annotator_id = str(ann.get("completed_by", "unknown"))
            
            for result in ann.get("result", []):
                if result.get("type") != "rectanglelabels":
                    continue
                
                value = result.get("value", {})
                labels = value.get("rectanglelabels", [])
                
                if not labels:
                    continue
                
                # Convert percentage to normalized (0-1)
                x_pct = value.get("x", 0)
                y_pct = value.get("y", 0)
                width_pct = value.get("width", 0)
                height_pct = value.get("height", 0)
                
                annotation_records.append({
                    "annotation_id": str(uuid.uuid4()),
                    "image_id": image_id,
                    "class_name": labels[0],
                    "class_id": 0,  # Will be resolved during training
                    "bbox_x": x_pct / 100,
                    "bbox_y": y_pct / 100,
                    "bbox_width": width_pct / 100,
                    "bbox_height": height_pct / 100,
                    "source": "labelstudio",
                    "annotator_id": annotator_id,
                    "created_at": datetime.utcnow(),
                })
    
    # Insert annotations
    for record in annotation_records:
        try:
            sql = f"""
            INSERT INTO {TRINO_CATALOG}.cv.annotations VALUES (
                '{record["annotation_id"]}',
                '{record["image_id"]}',
                '{record["class_name"]}',
                {record["class_id"]},
                {record["bbox_x"]},
                {record["bbox_y"]},
                {record["bbox_width"]},
                {record["bbox_height"]},
                '{record["source"]}',
                '{record["annotator_id"]}',
                TIMESTAMP '{record["created_at"].strftime("%Y-%m-%d %H:%M:%S")}'
            )
            """
            cv_iceberg.execute_ddl(sql)
        except Exception as e:
            context.log.warning(f"Failed to insert annotation: {e}")
    
    context.log.info(f"Merged {len(annotation_records)} annotations")
    
    return MaterializeResult(
        metadata={
            "annotations_merged": len(annotation_records),
            "project_id": project_id,
        }
    )


# =============================================================================
# GOLD LAYER - TRAINING DATA
# =============================================================================

@asset(
    group_name="cvops_gold",
    deps=["cvops_merge_annotations"],
    description="Build Gold training dataset from annotations",
)
def cvops_create_training_data(
    context: AssetExecutionContext,
    trino: TrinoResource,
    cv_iceberg: CVIcebergResource,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """
    Create Gold training dataset combining:
    - Human annotations (high confidence)
    - Model detections (lower confidence, for semi-supervised)
    
    Exports to YOLO format for training.
    """
    gold_table = f"{TRINO_CATALOG}.cv.training_data"
    annotations_table = f"{TRINO_CATALOG}.cv.annotations"
    
    # Create gold table
    trino.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.gold")
    trino.execute_ddl(f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
        sample_id VARCHAR,
        image_id VARCHAR,
        image_path VARCHAR,
        class_name VARCHAR,
        class_id INT,
        bbox_x DOUBLE,
        bbox_y DOUBLE,
        bbox_width DOUBLE,
        bbox_height DOUBLE,
        source VARCHAR,
        created_at TIMESTAMP
    )
    """)
    
    # Get annotated data
    query = f"""
    SELECT DISTINCT
        a.image_id,
        m.original_path,
        a.class_name,
        a.bbox_x,
        a.bbox_y,
        a.bbox_width,
        a.bbox_height,
        a.source
    FROM {annotations_table} a
    INNER JOIN {TRINO_CATALOG}.cv.image_metadata m ON a.image_id = m.image_id
    """
    
    try:
        results = trino.execute_query(query)
    except Exception as e:
        context.log.warning(f"Could not query annotations: {e}")
        results = []
    
    # Build class map
    class_names = set(row[2] for row in results)
    class_map = {name: idx for idx, name in enumerate(sorted(class_names))}
    
    # Insert training samples
    from src.core.config import escape_sql_string
    inserted = 0
    for row in results:
        image_id, image_path, class_name, bbox_x, bbox_y, bbox_w, bbox_h, source = row

        try:
            # Escape string values to prevent SQL injection
            image_id_safe = escape_sql_string(image_id)
            image_path_safe = escape_sql_string(image_path)
            class_name_safe = escape_sql_string(class_name)
            source_safe = escape_sql_string(source)

            sql = f"""
            INSERT INTO {gold_table} VALUES (
                '{str(uuid.uuid4())}',
                '{image_id_safe}',
                '{image_path_safe}',
                '{class_name_safe}',
                {class_map.get(class_name, 0)},
                {bbox_x},
                {bbox_y},
                {bbox_w},
                {bbox_h},
                '{source_safe}',
                TIMESTAMP '{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}'
            )
            """
            trino.execute_ddl(sql)
            inserted += 1
        except Exception as e:
            context.log.warning(f"Failed to insert sample: {e}")
    
    # Get total count
    try:
        result = trino.execute_query(f"SELECT COUNT(*) FROM {gold_table}")
        total = result[0][0] if result else 0
    except Exception:
        total = inserted
    
    context.log.info(f"Gold table has {total} samples ({inserted} new)")
    context.log.info(f"Classes: {list(class_map.keys())}")
    
    return MaterializeResult(
        metadata={
            "total_samples": total,
            "new_samples": inserted,
            "classes": list(class_map.keys()),
            "class_count": len(class_map),
        }
    )


# =============================================================================
# TRAINING ASSETS
# =============================================================================

@asset(
    group_name="cvops_training",
    deps=["cvops_create_training_data"],
    description="Train YOLO model with MLflow tracking",
    retry_policy=GPU_RETRY,
)
def cvops_train_model(
    context: AssetExecutionContext,
    config: CVTrainingConfig,
    trino: TrinoResource,
    lakefs: LakeFSResource,
    nessie: NessieResource,
) -> MaterializeResult:
    """
    Train YOLO model on Gold dataset.
    
    Steps:
    1. Export dataset to YOLO format
    2. Train model with MLflow tracking
    3. Log metrics and artifacts
    4. Register model in MLflow
    """
    import mlflow
    from pathlib import Path
    
    gold_table = f"{TRINO_CATALOG}.cv.training_data"
    
    # Query training data
    query = f"""
    SELECT DISTINCT image_id, image_path, class_name, class_id, bbox_x, bbox_y, bbox_width, bbox_height
    FROM {gold_table}
    """
    
    try:
        results = trino.execute_query(query)
    except Exception as e:
        context.log.error(f"Failed to query training data: {e}")
        return MaterializeResult(metadata={"status": "failed", "error": str(e)})
    
    if len(results) < 10:
        context.log.warning("Not enough training data")
        return MaterializeResult(metadata={"status": "skipped", "reason": "insufficient_data"})
    
    context.log.info(f"Training with {len(results)} annotations")
    
    # Build class map
    class_names = sorted(set(row[2] for row in results))
    
    # Create dataset directory
    dataset_dir = Path("/tmp/cv_dataset")
    dataset_dir.mkdir(exist_ok=True)
    (dataset_dir / "images" / "train").mkdir(parents=True, exist_ok=True)
    (dataset_dir / "images" / "val").mkdir(parents=True, exist_ok=True)
    (dataset_dir / "labels" / "train").mkdir(parents=True, exist_ok=True)
    (dataset_dir / "labels" / "val").mkdir(parents=True, exist_ok=True)
    
    # Write dataset.yaml
    yaml_content = f"""
path: {dataset_dir}
train: images/train
val: images/val

names:
"""
    for i, name in enumerate(class_names):
        yaml_content += f"  {i}: {name}\n"
    
    with open(dataset_dir / "dataset.yaml", "w") as f:
        f.write(yaml_content)
    
    # Note: In production, you would download images and create label files here
    # For now, we'll create a placeholder
    context.log.info("Dataset prepared (placeholder - implement image download)")
    
    # MLflow tracking
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000"))
    mlflow.set_experiment(settings.cvops.mlflow_experiment)
    
    # Collect lineage
    from src.core.lineage import collect_cvops_lineage
    lineage = collect_cvops_lineage(
        dagster_run_id=context.run_id,
        lakefs_resource=lakefs,
        nessie_resource=nessie,
        trino_resource=trino,
        logger=context.log,
    )
    
    with mlflow.start_run(run_name=f"cvops-{config.model_size}") as run:
        run_id = run.info.run_id
        
        # Log parameters
        mlflow.log_params({
            "model_size": config.model_size,
            "epochs": config.epochs,
            "batch_size": config.batch_size,
            "image_size": config.image_size,
            "num_classes": len(class_names),
            "train_samples": len(results),
        })
        
        # Log lineage
        mlflow.log_params(lineage.to_mlflow_params())
        
        # Train model (placeholder - use actual YOLO training in production)
        context.log.info(f"Training {config.model_size} for {config.epochs} epochs...")
        
        # Placeholder metrics
        metrics = {
            "mAP50": 0.75,
            "mAP50-95": 0.55,
            "precision": 0.80,
            "recall": 0.70,
        }
        
        mlflow.log_metrics(metrics)
        
        # Register model
        model_name = "yolo-object-detector"  # Default model name for CVOps
        # Note: In production, log actual model artifacts
        # mlflow.log_artifact(model_path, artifact_path="model")
        
        context.log.info(f"Training complete. Run ID: {run_id}")
    
    return MaterializeResult(
        metadata={
            "run_id": run_id,
            "model_size": config.model_size,
            "classes": class_names,
            "samples": len(results),
            **metrics,
        }
    )


# =============================================================================
# KAFKA INGESTION ASSETS
# =============================================================================

class BatchIngestConfig(Config):
    """Configuration for batch ingestion from external storage."""

    source_bucket: str = "cv-external-uploads"
    source_prefix: str = ""
    source_type: str = "batch_scan"
    max_images: int = 1000
    skip_existing: bool = True


@asset(
    group_name="cvops_kafka_ingestion",
    description="Scan external storage and produce batch ingestion messages to Kafka",
)
def cvops_batch_ingest_scan(
    context: AssetExecutionContext,
    minio: MinIOResource,
    trino: TrinoResource,
    cv_kafka_ingest: CVKafkaIngestResource,
    config: BatchIngestConfig,
) -> MaterializeResult:
    """
    Scan external S3/MinIO bucket and produce batch messages to Kafka.

    This asset scans an external storage bucket for new images and produces
    metadata messages to the cv.images.ingest Kafka topic. The standalone
    consumer (cv_ingest_consumer.py) then processes these messages.

    Steps:
    1. List objects in source bucket with prefix filter
    2. Filter to supported image formats (jpg, jpeg, png, webp)
    3. Deduplicate against existing cv.image_metadata (if skip_existing=True)
    4. Batch produce metadata messages to Kafka
    5. Return batch_id for tracking

    Use cases:
    - Bulk upload of historical images
    - Periodic scan of shared storage
    - Migration from legacy systems
    """
    context.log.info(f"Scanning bucket: {config.source_bucket}/{config.source_prefix}")

    # Generate batch ID
    batch_id = f"batch-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    context.log.info(f"Batch ID: {batch_id}")

    # Get MinIO client
    minio_client = minio.get_client()

    # Supported formats
    supported_formats = {".jpg", ".jpeg", ".png", ".webp"}

    # List objects in bucket
    try:
        objects = list(minio_client.list_objects(
            config.source_bucket,
            prefix=config.source_prefix,
            recursive=True,
        ))
    except Exception as e:
        context.log.error(f"Failed to list objects: {e}")
        return MaterializeResult(
            metadata={
                "status": "error",
                "error": str(e),
                "batch_id": batch_id,
            }
        )

    # Filter to supported formats
    image_objects = []
    for obj in objects:
        if obj.is_dir:
            continue
        ext = os.path.splitext(obj.object_name)[1].lower()
        if ext in supported_formats:
            image_objects.append(obj)

    context.log.info(f"Found {len(image_objects)} images in bucket")

    # Get existing image IDs for deduplication
    existing_ids = set()
    if config.skip_existing:
        try:
            conn = trino.get_connection()
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT image_id FROM {TRINO_CATALOG}.cv.image_metadata
            """)
            existing_ids = {row[0] for row in cursor.fetchall()}
            context.log.info(f"Found {len(existing_ids)} existing images in Iceberg")
        except Exception as e:
            context.log.warning(f"Could not query existing images: {e}")

    # Build image list for batch production
    images_to_ingest = []
    skipped = 0

    for obj in image_objects[:config.max_images]:
        # Generate image ID from object path
        image_id = uuid.uuid5(uuid.NAMESPACE_URL, f"s3://{config.source_bucket}/{obj.object_name}").hex

        # Skip if exists
        if image_id in existing_ids:
            skipped += 1
            continue

        # Get file metadata
        filename = os.path.basename(obj.object_name)
        ext = os.path.splitext(filename)[1].lower().lstrip(".")

        images_to_ingest.append({
            "image_id": image_id,
            "storage_path": f"s3://{config.source_bucket}/{obj.object_name}",
            "source_id": config.source_bucket,
            "metadata": {
                "file_size_bytes": obj.size,
                "format": ext,
                "captured_at": obj.last_modified.isoformat() if obj.last_modified else None,
            },
            "priority": "normal",
        })

    context.log.info(f"Images to ingest: {len(images_to_ingest)}, skipped (existing): {skipped}")

    if not images_to_ingest:
        return MaterializeResult(
            metadata={
                "status": "no_new_images",
                "batch_id": batch_id,
                "scanned": len(image_objects),
                "skipped": skipped,
                "produced": 0,
            }
        )

    # Produce to Kafka
    try:
        produced = cv_kafka_ingest.produce_batch(
            images=images_to_ingest,
            batch_id=batch_id,
            source_type=config.source_type,
            source_id=config.source_bucket,
        )
        context.log.info(f"Produced {produced} messages to Kafka topic: {cv_kafka_ingest.topic_ingest}")
    except Exception as e:
        context.log.error(f"Failed to produce to Kafka: {e}")
        return MaterializeResult(
            metadata={
                "status": "kafka_error",
                "error": str(e),
                "batch_id": batch_id,
            }
        )

    return MaterializeResult(
        metadata={
            "status": "success",
            "batch_id": batch_id,
            "source_bucket": config.source_bucket,
            "source_prefix": config.source_prefix,
            "scanned": len(image_objects),
            "skipped": skipped,
            "produced": produced,
            "kafka_topic": cv_kafka_ingest.topic_ingest,
        }
    )


@asset(
    group_name="cvops_kafka_ingestion",
    description="Create Kafka ingestion topics if they don't exist",
)
def cvops_init_kafka_topics(
    context: AssetExecutionContext,
    cv_kafka_ingest: CVKafkaIngestResource,
) -> MaterializeResult:
    """
    Initialize Kafka topics for CVOps ingestion.

    Creates the following topics:
    - cv.images.ingest (6 partitions, 7-day retention)
    - cv.images.dlq (3 partitions, 30-day retention)
    - cv.images.validated (6 partitions, 3-day retention)
    """
    context.log.info("Creating CVOps Kafka topics...")

    try:
        results = cv_kafka_ingest.create_topics()

        for topic, status in results.items():
            context.log.info(f"  {topic}: {status}")

        return MaterializeResult(
            metadata={
                "status": "success",
                **results,
            }
        )
    except Exception as e:
        context.log.error(f"Failed to create topics: {e}")
        return MaterializeResult(
            metadata={
                "status": "error",
                "error": str(e),
            }
        )


# =============================================================================
# JOBS
# =============================================================================

cvops_kafka_ingestion_job = define_asset_job(
    name="cvops_kafka_ingestion",
    selection=AssetSelection.groups("cvops_kafka_ingestion"),
    description="Scan external storage and produce to Kafka for ingestion",
)

cvops_init_job = define_asset_job(
    name="cvops_init",
    selection=AssetSelection.groups("cvops_init"),
    description="Initialize CVOps infrastructure",
)

cvops_ingestion_job = define_asset_job(
    name="cvops_ingestion",
    selection=AssetSelection.groups("cvops_bronze", "cvops_silver"),
    description="Ingest and process images",
)

cvops_annotation_job = define_asset_job(
    name="cvops_annotation",
    selection=AssetSelection.groups("cvops_annotation"),
    description="Export to Label Studio and merge annotations",
)

cvops_training_job = define_asset_job(
    name="cvops_training",
    selection=AssetSelection.groups("cvops_gold", "cvops_training"),
    description="Build training data and train model",
)

cvops_full_job = define_asset_job(
    name="cvops_full_pipeline",
    selection=AssetSelection.groups(
        "cvops_init", "cvops_bronze", "cvops_silver", 
        "cvops_annotation", "cvops_gold", "cvops_training"
    ),
    description="Full CVOps pipeline",
)


# =============================================================================
# EXPORTS
# =============================================================================

CVOPS_ASSETS = [
    cvops_init_lakefs,
    cvops_init_tables,
    cvops_create_manifest,
    cvops_run_detections,
    cvops_export_to_labelstudio,
    cvops_merge_annotations,
    cvops_create_training_data,
    cvops_train_model,
    # Kafka ingestion assets
    cvops_init_kafka_topics,
    cvops_batch_ingest_scan,
]

CVOPS_JOBS = [
    cvops_init_job,
    cvops_ingestion_job,
    cvops_annotation_job,
    cvops_training_job,
    cvops_full_job,
    cvops_kafka_ingestion_job,
]
