#!/usr/bin/env python3
"""
CVOps End-to-End Test Script
============================

Tests the complete CVOps pipeline from image ingestion to Gold training data.

Pipeline Flow:
    Images (MinIO) -> LakeFS (versioned) -> Iceberg (cv.image_metadata)
    -> YOLO Detection (cv.detection_results) -> Label Studio -> Gold Data

Usage:
    python scripts/test_cvops_e2e.py
"""

import os
import sys
import json
import uuid
import time
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import requests

# MinIO/S3 configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:19000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")

# LakeFS configuration
LAKEFS_ENDPOINT = os.getenv("LAKEFS_ENDPOINT", "http://localhost:18000")
LAKEFS_ACCESS_KEY = os.getenv("LAKEFS_ACCESS_KEY_ID", "YOUR_LAKEFS_ACCESS_KEY_HERE")
LAKEFS_SECRET_KEY = os.getenv("LAKEFS_SECRET_ACCESS_KEY", "YOUR_LAKEFS_SECRET_KEY_HERE")

# CV API configuration
CV_API_URL = os.getenv("CV_API_URL", "http://localhost:8002")

# Trino configuration
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = os.getenv("TRINO_PORT", "18083")
TRINO_USER = os.getenv("TRINO_USER", "admin")


def get_boto3_client(endpoint, access_key, secret_key):
    """Create boto3 S3 client."""
    import boto3
    from botocore.config import Config

    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def get_lakefs_client():
    """Create LakeFS S3-compatible client."""
    return get_boto3_client(
        f"{LAKEFS_ENDPOINT}/api/v1",
        LAKEFS_ACCESS_KEY,
        LAKEFS_SECRET_KEY
    )


def get_minio_client():
    """Create MinIO S3 client."""
    return get_boto3_client(
        MINIO_ENDPOINT,
        MINIO_ACCESS_KEY,
        MINIO_SECRET_KEY
    )


def lakefs_api(method, path, **kwargs):
    """Make LakeFS API request."""
    url = f"{LAKEFS_ENDPOINT}/api/v1{path}"
    auth = (LAKEFS_ACCESS_KEY, LAKEFS_SECRET_KEY)
    response = requests.request(method, url, auth=auth, **kwargs)
    return response


def check_services():
    """Check that all required services are running."""
    print("=" * 60)
    print("Checking Services")
    print("=" * 60)

    services = {
        "MinIO": f"{MINIO_ENDPOINT}/minio/health/live",
        "LakeFS": f"{LAKEFS_ENDPOINT}/api/v1/healthcheck",
        "CV API": f"{CV_API_URL}/health",
    }

    all_healthy = True
    for name, url in services.items():
        try:
            resp = requests.get(url, timeout=5)
            # 200 and 204 are both healthy responses
            if resp.status_code in (200, 204):
                print(f"  OK {name}: healthy")
            else:
                print(f"  FAIL {name}: status {resp.status_code}")
                all_healthy = False
        except Exception as e:
            print(f"  FAIL {name}: {e}")
            all_healthy = False

    return all_healthy


def copy_images_to_lakefs():
    """Copy test images from MinIO to LakeFS."""
    print()
    print("=" * 60)
    print("Copying Images to LakeFS")
    print("=" * 60)

    minio = get_minio_client()

    # List images in MinIO cv-raw/test-images/
    try:
        response = minio.list_objects_v2(
            Bucket='cv-raw',
            Prefix='test-images/'
        )
        objects = response.get('Contents', [])
        print(f"  Found {len(objects)} images in MinIO cv-raw/test-images/")
    except Exception as e:
        print(f"  ERROR: Cannot list MinIO objects: {e}")
        return []

    if not objects:
        print("  No images found. Run setup_cvops_test_images.py first.")
        return []

    # Upload each image to LakeFS
    uploaded = []
    for obj in objects:
        key = obj['Key']
        filename = os.path.basename(key)
        lakefs_path = f"raw/images/{filename}"

        try:
            # Download from MinIO
            img_data = minio.get_object(Bucket='cv-raw', Key=key)['Body'].read()

            # Upload to LakeFS via S3 API
            # LakeFS uses format: s3://repo/branch/path
            lakefs_url = f"{LAKEFS_ENDPOINT}/api/v1/repositories/cv-data/branches/dev-cvops/objects?path={lakefs_path}"
            resp = requests.post(
                lakefs_url,
                auth=(LAKEFS_ACCESS_KEY, LAKEFS_SECRET_KEY),
                headers={'Content-Type': 'image/jpeg'},
                data=img_data
            )

            if resp.status_code in (200, 201):
                print(f"    Uploaded: {filename}")
                uploaded.append(lakefs_path)
            else:
                print(f"    FAIL {filename}: {resp.status_code} - {resp.text[:100]}")
        except Exception as e:
            print(f"    ERROR {filename}: {e}")

    print(f"  Uploaded {len(uploaded)} images to LakeFS cv-data/dev-cvops")
    return uploaded


def commit_lakefs():
    """Commit changes to LakeFS."""
    print()
    print("=" * 60)
    print("Committing to LakeFS")
    print("=" * 60)

    commit_data = {
        "message": f"CVOps test images - {datetime.utcnow().isoformat()}",
        "metadata": {
            "source": "cvops_e2e_test",
            "pipeline": "cvops",
        }
    }

    resp = lakefs_api(
        "POST",
        "/repositories/cv-data/branches/dev-cvops/commits",
        json=commit_data
    )

    if resp.status_code == 201:
        commit = resp.json()
        commit_id = commit.get("id", "unknown")
        print(f"  Committed: {commit_id}")
        return commit_id
    else:
        print(f"  FAIL: {resp.status_code} - {resp.text}")
        return None


def check_trino_tables():
    """Check Iceberg tables exist in Trino."""
    print()
    print("=" * 60)
    print("Checking Iceberg Tables")
    print("=" * 60)

    try:
        import trino
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user=TRINO_USER,
            catalog='iceberg_dev',
            schema='cv'
        )
        cursor = conn.cursor()

        tables = ['image_metadata', 'detection_results', 'annotations', 'training_data']

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM iceberg_dev.cv.{table}")
                count = cursor.fetchone()[0]
                print(f"  OK cv.{table}: {count} rows")
            except Exception as e:
                print(f"  MISSING cv.{table}: {e}")

        cursor.close()
        conn.close()
        return True
    except ImportError:
        print("  SKIP: trino package not installed")
        return False
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def ingest_to_iceberg(images, commit_id):
    """Insert image metadata into Iceberg table."""
    print()
    print("=" * 60)
    print("Ingesting to Iceberg")
    print("=" * 60)

    try:
        import trino
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user=TRINO_USER,
            catalog='iceberg_dev',
            schema='cv'
        )
        cursor = conn.cursor()

        inserted = 0
        for path in images:
            image_id = str(uuid.uuid4())
            filename = os.path.basename(path)
            storage_path = f"lakefs://cv-data/dev-cvops/{path}"

            sql = f"""
            INSERT INTO iceberg_dev.cv.image_metadata (
                image_id, original_path, storage_path, filename, format,
                source_type, source_id, processed, source_lakefs_commit,
                source_lakefs_branch, ingested_at
            ) VALUES (
                '{image_id}',
                '{storage_path}',
                '{storage_path}',
                '{filename}',
                'jpeg',
                'batch_test',
                'cv-raw',
                false,
                '{commit_id or ""}',
                'dev-cvops',
                TIMESTAMP '{datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}'
            )
            """
            try:
                cursor.execute(sql)
                inserted += 1
                print(f"    Inserted: {filename}")
            except Exception as e:
                print(f"    FAIL {filename}: {e}")

        cursor.close()
        conn.close()
        print(f"  Inserted {inserted} images into cv.image_metadata")
        return inserted
    except ImportError:
        print("  SKIP: trino package not installed")
        return 0
    except Exception as e:
        print(f"  ERROR: {e}")
        return 0


def run_yolo_detection():
    """Run YOLO detection via CV API on unprocessed images."""
    print()
    print("=" * 60)
    print("Running YOLO Detection")
    print("=" * 60)

    try:
        import trino
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user=TRINO_USER,
            catalog='iceberg_dev',
            schema='cv'
        )
        cursor = conn.cursor()

        # Get unprocessed images
        cursor.execute("""
            SELECT image_id, storage_path, filename
            FROM iceberg_dev.cv.image_metadata
            WHERE processed = false
            LIMIT 20
        """)
        images = cursor.fetchall()
        print(f"  Found {len(images)} unprocessed images")

        if not images:
            return 0

        minio = get_minio_client()
        detections_total = 0

        for image_id, storage_path, filename in images:
            try:
                # Download image from MinIO (test-images location)
                img_data = minio.get_object(
                    Bucket='cv-raw',
                    Key=f'test-images/{filename}'
                )['Body'].read()

                # Send to CV API for detection
                files = {'file': (filename, img_data, 'image/jpeg')}
                resp = requests.post(
                    f"{CV_API_URL}/detect",
                    files=files,
                    timeout=30
                )

                if resp.status_code == 200:
                    result = resp.json()
                    detections = result.get('detections', [])
                    inference_ms = result.get('inference_time_ms', 0)

                    # Insert detections
                    for det in detections:
                        det_id = str(uuid.uuid4())
                        # CV API returns normalized coords: bbox_x, bbox_y, bbox_width, bbox_height
                        bbox_x = det.get('bbox_x', 0)
                        bbox_y = det.get('bbox_y', 0)
                        bbox_w = det.get('bbox_width', 0)
                        bbox_h = det.get('bbox_height', 0)
                        # Convert to x1,y1,x2,y2 format
                        x1, y1 = bbox_x, bbox_y
                        x2, y2 = bbox_x + bbox_w, bbox_y + bbox_h

                        sql = f"""
                        INSERT INTO iceberg_dev.cv.detection_results (
                            detection_id, image_id, model_name, model_version,
                            class_id, class_name, confidence,
                            bbox_x1, bbox_y1, bbox_x2, bbox_y2,
                            bbox_width, bbox_height, inference_ms, detected_at
                        ) VALUES (
                            '{det_id}',
                            '{image_id}',
                            'yolov8n',
                            'local',
                            {det.get('class_id', 0)},
                            '{det.get('class_name', 'unknown')}',
                            {det.get('confidence', 0)},
                            {x1},
                            {y1},
                            {x2},
                            {y2},
                            {bbox_w},
                            {bbox_h},
                            {inference_ms},
                            TIMESTAMP '{datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")}'
                        )
                        """
                        cursor.execute(sql)
                        detections_total += 1

                    # Mark as processed
                    cursor.execute(f"""
                        UPDATE iceberg_dev.cv.image_metadata
                        SET processed = true
                        WHERE image_id = '{image_id}'
                    """)

                    print(f"    {filename}: {len(detections)} detections")
                else:
                    print(f"    FAIL {filename}: {resp.status_code}")

            except Exception as e:
                print(f"    ERROR {filename}: {e}")

        cursor.close()
        conn.close()
        print(f"  Total detections: {detections_total}")
        return detections_total

    except ImportError:
        print("  SKIP: trino package not installed")
        return 0
    except Exception as e:
        print(f"  ERROR: {e}")
        return 0


def show_summary():
    """Show pipeline summary."""
    print()
    print("=" * 60)
    print("Pipeline Summary")
    print("=" * 60)

    try:
        import trino
        conn = trino.dbapi.connect(
            host=TRINO_HOST,
            port=int(TRINO_PORT),
            user=TRINO_USER,
            catalog='iceberg_dev',
            schema='cv'
        )
        cursor = conn.cursor()

        # Image metadata
        cursor.execute("SELECT COUNT(*), COUNT(CASE WHEN processed THEN 1 END) FROM iceberg_dev.cv.image_metadata")
        total, processed = cursor.fetchone()
        print(f"  Images: {total} total, {processed} processed")

        # Detections
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT class_name) FROM iceberg_dev.cv.detection_results")
        det_count, class_count = cursor.fetchone()
        print(f"  Detections: {det_count} total, {class_count} classes")

        # Top classes
        cursor.execute("""
            SELECT class_name, COUNT(*), ROUND(AVG(confidence), 3)
            FROM iceberg_dev.cv.detection_results
            GROUP BY class_name
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        top_classes = cursor.fetchall()
        if top_classes:
            print("  Top classes:")
            for name, count, conf in top_classes:
                print(f"    - {name}: {count} ({conf:.1%} avg confidence)")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"  Could not get summary: {e}")


def main():
    print("=" * 60)
    print("CVOps End-to-End Pipeline Test")
    print("=" * 60)
    print(f"Started: {datetime.utcnow().isoformat()}")
    print()

    # 1. Check services
    if not check_services():
        print("\nSome services are not healthy. Please start them first:")
        print("  docker compose -f docker-compose.core.yml -f docker-compose.cv.yml up -d")
        return

    # 2. Check tables exist
    check_trino_tables()

    # 3. Copy images to LakeFS
    images = copy_images_to_lakefs()
    if not images:
        print("\nNo images to process. Run setup_cvops_test_images.py first.")
        return

    # 4. Commit to LakeFS
    commit_id = commit_lakefs()

    # 5. Ingest to Iceberg
    ingest_to_iceberg(images, commit_id)

    # 6. Run YOLO detection
    run_yolo_detection()

    # 7. Summary
    show_summary()

    print()
    print("=" * 60)
    print("Next Steps")
    print("=" * 60)
    print("  1. Open Label Studio at http://localhost:8080")
    print("  2. Review auto-annotated images")
    print("  3. Run cvops_annotation_job to merge annotations")
    print("  4. Run cvops_training_job to create gold data")
    print()


if __name__ == "__main__":
    main()
