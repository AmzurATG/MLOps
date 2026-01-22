#!/usr/bin/env python3
"""
Download COCO Sample Images for CVOps Testing
==============================================

Downloads a sample of COCO val2017 images and uploads them to MinIO
for testing the CVOps pipeline.

Usage:
    python scripts/download_coco_sample.py --count 100
    python scripts/download_coco_sample.py --count 50 --upload-only  # Skip download if already have images

Requirements:
    pip install boto3 requests tqdm
"""

import os
import json
import argparse
import tempfile
import zipfile
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from tqdm import tqdm

# Try to import boto3 for MinIO upload
try:
    import boto3
    from botocore.config import Config
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    print("Warning: boto3 not installed. Install with: pip install boto3")


# COCO dataset URLs
COCO_VAL_IMAGES_URL = "http://images.cocodataset.org/zips/val2017.zip"
COCO_ANNOTATIONS_URL = "http://images.cocodataset.org/annotations/annotations_trainval2017.zip"

# MinIO configuration (defaults match docker-compose)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:19000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
MINIO_BUCKET = "cv-raw"


def download_file(url: str, dest_path: Path, desc: str = "Downloading") -> Path:
    """Download a file with progress bar."""
    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))

    with open(dest_path, 'wb') as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc=desc) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                pbar.update(len(chunk))

    return dest_path


def extract_sample_images(zip_path: Path, output_dir: Path, count: int) -> list:
    """Extract a sample of images from the COCO zip file."""
    extracted = []

    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Get all image files
        image_files = [f for f in zf.namelist() if f.endswith('.jpg')]

        # Take first N images
        sample_files = image_files[:count]

        print(f"Extracting {len(sample_files)} images...")
        for img_file in tqdm(sample_files, desc="Extracting"):
            # Extract to output directory
            zf.extract(img_file, output_dir)
            extracted.append(output_dir / img_file)

    return extracted


def get_minio_client():
    """Create MinIO client using boto3."""
    if not BOTO3_AVAILABLE:
        raise RuntimeError("boto3 not installed")

    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def upload_to_minio(local_path: Path, s3_key: str, client) -> bool:
    """Upload a file to MinIO."""
    try:
        client.upload_file(str(local_path), MINIO_BUCKET, s3_key)
        return True
    except Exception as e:
        print(f"Failed to upload {local_path}: {e}")
        return False


def upload_images_parallel(image_paths: list, prefix: str = "coco-sample/images/", workers: int = 10):
    """Upload images to MinIO in parallel."""
    if not BOTO3_AVAILABLE:
        print("Skipping upload - boto3 not installed")
        return []

    client = get_minio_client()
    uploaded = []

    def upload_one(img_path: Path):
        filename = img_path.name
        s3_key = f"{prefix}{filename}"
        success = upload_to_minio(img_path, s3_key, client)
        return (img_path, s3_key, success)

    print(f"Uploading {len(image_paths)} images to MinIO...")
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(upload_one, p): p for p in image_paths}

        with tqdm(total=len(futures), desc="Uploading") as pbar:
            for future in as_completed(futures):
                img_path, s3_key, success = future.result()
                if success:
                    uploaded.append((img_path, s3_key))
                pbar.update(1)

    return uploaded


def create_manifest(uploaded_files: list, output_path: Path):
    """Create a manifest JSON file for the uploaded images."""
    manifest = {
        "created_at": datetime.utcnow().isoformat(),
        "source": "COCO val2017",
        "bucket": MINIO_BUCKET,
        "count": len(uploaded_files),
        "images": []
    }

    for local_path, s3_key in uploaded_files:
        manifest["images"].append({
            "image_id": local_path.stem,
            "filename": local_path.name,
            "s3_key": s3_key,
            "s3_uri": f"s3://{MINIO_BUCKET}/{s3_key}"
        })

    with open(output_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"Manifest saved to: {output_path}")
    return manifest


def main():
    parser = argparse.ArgumentParser(description="Download COCO sample images for CVOps testing")
    parser.add_argument("--count", type=int, default=100, help="Number of images to download")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory for images")
    parser.add_argument("--upload-only", action="store_true", help="Skip download, only upload existing images")
    parser.add_argument("--no-upload", action="store_true", help="Skip MinIO upload")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel upload workers")
    args = parser.parse_args()

    # Set up output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="coco_sample_"))

    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}")

    # Check for existing images
    existing_images = list((output_dir / "val2017").glob("*.jpg")) if (output_dir / "val2017").exists() else []

    if args.upload_only and existing_images:
        print(f"Using {len(existing_images)} existing images")
        image_paths = existing_images[:args.count]
    elif args.upload_only:
        print("No existing images found. Run without --upload-only first.")
        return
    else:
        # Download COCO val2017 images
        zip_path = output_dir / "val2017.zip"

        if not zip_path.exists():
            print("Downloading COCO val2017 images (this may take a while)...")
            print(f"URL: {COCO_VAL_IMAGES_URL}")
            download_file(COCO_VAL_IMAGES_URL, zip_path, "Downloading COCO val2017")
        else:
            print(f"Using existing zip file: {zip_path}")

        # Extract sample
        image_paths = extract_sample_images(zip_path, output_dir, args.count)
        print(f"Extracted {len(image_paths)} images")

    # Upload to MinIO
    if not args.no_upload:
        if BOTO3_AVAILABLE:
            uploaded = upload_images_parallel(image_paths, workers=args.workers)
            print(f"Uploaded {len(uploaded)} images to MinIO bucket: {MINIO_BUCKET}")

            # Create manifest
            manifest_path = output_dir / "manifest.json"
            create_manifest(uploaded, manifest_path)
        else:
            print("Skipping upload - boto3 not installed")
            print("Install with: pip install boto3")

    print("\n=== Summary ===")
    print(f"Images extracted: {len(image_paths)}")
    print(f"Output directory: {output_dir}")
    print(f"MinIO bucket: s3://{MINIO_BUCKET}/coco-sample/images/")

    print("\n=== Next Steps ===")
    print("1. Run Dagster cvops_init_job to create Iceberg tables")
    print("2. Run Dagster cvops_ingestion_job to process images")
    print("3. Check cv.image_metadata table for ingested images")


if __name__ == "__main__":
    main()
