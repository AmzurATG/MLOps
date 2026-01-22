#!/usr/bin/env python3
"""
Setup CVOps Test Images
=======================

Downloads sample images from public URLs and uploads them to MinIO
for testing the CVOps pipeline.

This is a faster alternative to downloading the full COCO dataset.

Usage:
    python scripts/setup_cvops_test_images.py
"""

import os
import json
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.config import Config
    BOTO3_AVAILABLE = True
except ImportError:
    print("Installing boto3...")
    import subprocess
    subprocess.check_call(['pip', 'install', 'boto3', '-q'])
    import boto3
    from botocore.config import Config
    BOTO3_AVAILABLE = True

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:19000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
MINIO_BUCKET = "cv-raw"

# Sample images from Unsplash (free, fast CDN)
SAMPLE_IMAGES = [
    ("unsplash_dog_001.jpg", "https://images.unsplash.com/photo-1587300003388-59208cc962cb?w=640"),
    ("unsplash_cat_001.jpg", "https://images.unsplash.com/photo-1514888286974-6c03e2ca1dba?w=640"),
    ("unsplash_car_001.jpg", "https://images.unsplash.com/photo-1494976388531-d1058494cdd8?w=640"),
    ("unsplash_person_001.jpg", "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=640"),
    ("unsplash_traffic_001.jpg", "https://images.unsplash.com/photo-1449965408869-eaa3f722e40d?w=640"),
    ("unsplash_street_001.jpg", "https://images.unsplash.com/photo-1480714378408-67cf0d13bc1b?w=640"),
    ("unsplash_people_001.jpg", "https://images.unsplash.com/photo-1529156069898-49953e39b3ac?w=640"),
    ("unsplash_dog_002.jpg", "https://images.unsplash.com/photo-1561037404-61cd46aa615b?w=640"),
    ("unsplash_cat_002.jpg", "https://images.unsplash.com/photo-1573865526739-10659fec78a5?w=640"),
    ("unsplash_car_002.jpg", "https://images.unsplash.com/photo-1503376780353-7e6692767b70?w=640"),
    ("unsplash_person_002.jpg", "https://images.unsplash.com/photo-1500648767791-00dcc994a43e?w=640"),
    ("unsplash_city_001.jpg", "https://images.unsplash.com/photo-1449824913935-59a10b8d2000?w=640"),
    ("unsplash_dog_003.jpg", "https://images.unsplash.com/photo-1534361960057-19889db9621e?w=640"),
    ("unsplash_bicycle_001.jpg", "https://images.unsplash.com/photo-1485965120184-e220f721d03e?w=640"),
    ("unsplash_bus_001.jpg", "https://images.unsplash.com/photo-1544620347-c4fd4a3d5957?w=640"),
    ("unsplash_boat_001.jpg", "https://images.unsplash.com/photo-1544551763-46a013bb70d5?w=640"),
    ("unsplash_bird_001.jpg", "https://images.unsplash.com/photo-1444464666168-49d633b86797?w=640"),
    ("unsplash_horse_001.jpg", "https://images.unsplash.com/photo-1553284965-83fd3e82fa5a?w=640"),
    ("unsplash_train_001.jpg", "https://images.unsplash.com/photo-1474487548417-781cb71495f3?w=640"),
    ("unsplash_airplane_001.jpg", "https://images.unsplash.com/photo-1436491865332-7a61a109cc05?w=640"),
]


def get_minio_client():
    """Create MinIO client."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def download_and_upload(name: str, url: str, client, prefix: str = "test-images/") -> dict:
    """Download an image from URL and upload to MinIO."""
    try:
        # Download
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        image_data = response.content

        # Upload to MinIO
        s3_key = f"{prefix}{name}"
        client.put_object(
            Bucket=MINIO_BUCKET,
            Key=s3_key,
            Body=image_data,
            ContentType='image/jpeg'
        )

        return {
            "success": True,
            "name": name,
            "s3_key": s3_key,
            "size_bytes": len(image_data),
            "s3_uri": f"s3://{MINIO_BUCKET}/{s3_key}"
        }
    except Exception as e:
        return {
            "success": False,
            "name": name,
            "error": str(e)
        }


def main():
    print("=" * 60)
    print("CVOps Test Image Setup")
    print("=" * 60)
    print()

    # Create client
    client = get_minio_client()

    # Check bucket exists
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
        print(f"✓ MinIO bucket '{MINIO_BUCKET}' exists")
    except Exception as e:
        print(f"✗ MinIO bucket '{MINIO_BUCKET}' not found: {e}")
        print("  Run: docker compose -f docker-compose.cv.yml up exp-minio-cv-init")
        return

    # Download and upload images
    print(f"\nDownloading and uploading {len(SAMPLE_IMAGES)} test images...")
    print()

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(download_and_upload, name, url, client): name
            for name, url in SAMPLE_IMAGES
        }

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if result["success"]:
                print(f"  ✓ {result['name']} ({result['size_bytes']:,} bytes)")
            else:
                print(f"  ✗ {result['name']}: {result['error']}")

    # Summary
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Uploaded: {len(successful)}/{len(SAMPLE_IMAGES)} images")
    if failed:
        print(f"  Failed: {len(failed)} images")

    # Create manifest
    if successful:
        manifest = {
            "created_at": datetime.utcnow().isoformat(),
            "bucket": MINIO_BUCKET,
            "prefix": "test-images/",
            "count": len(successful),
            "images": [
                {
                    "image_id": r["name"].replace(".jpg", ""),
                    "filename": r["name"],
                    "s3_key": r["s3_key"],
                    "s3_uri": r["s3_uri"],
                    "size_bytes": r["size_bytes"]
                }
                for r in successful
            ]
        }

        manifest_path = Path("/tmp/cvops_test_manifest.json")
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)
        print(f"\n  Manifest saved to: {manifest_path}")

    print()
    print("=" * 60)
    print("Next Steps")
    print("=" * 60)
    print("  1. Run Dagster cvops_init_job to initialize tables")
    print("  2. The images are ready at: s3://cv-raw/test-images/")
    print("  3. Run cvops_ingestion_job to process the images")
    print()


if __name__ == "__main__":
    main()
