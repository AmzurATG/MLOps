#!/usr/bin/env python3
"""
Setup CVOps Label Studio Project
================================

Creates a Label Studio project for object detection review and exports
YOLO detections as pre-annotations.

Usage:
    python scripts/setup_cvops_labelstudio.py
"""

import os
import sys
import json
import requests
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Label Studio configuration
LABELSTUDIO_URL = os.getenv("LABELSTUDIO_URL", "http://localhost:18081")
LABELSTUDIO_API_TOKEN = os.getenv(
    "LABELSTUDIO_API_TOKEN",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6ODA3NTYwNTM5NCwiaWF0IjoxNzY4NDA1Mzk0LCJqdGkiOiJiNGVjODQwY2MzMjE0YjJmOWY0M2RmMWRlYTJmOTg0ZiIsInVzZXJfaWQiOjF9.y7tijAMLaDMoc2Q1Oi2m4_HngHilbAPiCRQ1JSugLp0"
)

# MinIO configuration - use localhost URL for browser access (bucket must be public)
MINIO_ENDPOINT = os.getenv("MINIO_BROWSER_ENDPOINT", "http://localhost:19000")

# Trino configuration
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = os.getenv("TRINO_PORT", "18083")
TRINO_USER = os.getenv("TRINO_USER", "admin")

# Project settings
PROJECT_NAME = "CVOps Object Detection Review"
LABEL_CONFIG = """
<View>
  <Image name="image" value="$image_url"/>
  <RectangleLabels name="label" toName="image">
    <Label value="person" background="#FF0000"/>
    <Label value="car" background="#0000FF"/>
    <Label value="truck" background="#00FFFF"/>
    <Label value="bus" background="#FF00FF"/>
    <Label value="motorcycle" background="#FFFF00"/>
    <Label value="bicycle" background="#00FF00"/>
    <Label value="dog" background="#FFA500"/>
    <Label value="cat" background="#800080"/>
    <Label value="bird" background="#008000"/>
    <Label value="horse" background="#A52A2A"/>
    <Label value="boat" background="#808080"/>
    <Label value="airplane" background="#FFD700"/>
    <Label value="train" background="#4B0082"/>
    <Label value="traffic light" background="#DC143C"/>
    <Label value="stop sign" background="#FF4500"/>
  </RectangleLabels>
</View>
"""


def get_access_token():
    """Get JWT access token from refresh token."""
    try:
        response = requests.post(
            f"{LABELSTUDIO_URL}/api/token/refresh",
            json={"refresh": LABELSTUDIO_API_TOKEN},
            timeout=10,
        )
        if response.ok:
            return response.json().get("access")
    except Exception:
        pass
    return None


def get_headers():
    """Get authenticated headers for Label Studio API."""
    access = get_access_token()
    if access:
        return {"Authorization": f"Bearer {access}", "Content-Type": "application/json"}
    # Fallback to token auth
    return {"Authorization": f"Token {LABELSTUDIO_API_TOKEN}", "Content-Type": "application/json"}


def get_or_create_project():
    """Get or create the CVOps project."""
    headers = get_headers()

    # List existing projects
    resp = requests.get(f"{LABELSTUDIO_URL}/api/projects/", headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"  ERROR: Could not list projects: {resp.status_code} - {resp.text}")
        return None

    projects = resp.json()
    if isinstance(projects, dict):
        projects = projects.get("results", [])

    # Check if project exists
    for project in projects:
        if project.get("title") == PROJECT_NAME:
            print(f"  Found existing project: {project['id']}")
            return project["id"]

    # Create new project
    print(f"  Creating new project: {PROJECT_NAME}")
    resp = requests.post(
        f"{LABELSTUDIO_URL}/api/projects/",
        headers=headers,
        json={
            "title": PROJECT_NAME,
            "label_config": LABEL_CONFIG,
            "description": "Object detection review with YOLO pre-annotations",
        },
        timeout=30,
    )

    if resp.status_code in (200, 201):
        project_id = resp.json()["id"]
        print(f"  Created project: {project_id}")
        return project_id
    else:
        print(f"  ERROR: Could not create project: {resp.status_code} - {resp.text}")
        return None


def get_images_and_detections():
    """Get images and their detections from Iceberg."""
    import trino

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user=TRINO_USER,
        catalog='iceberg_dev',
        schema='cv'
    )
    cursor = conn.cursor()

    # Get images with detections
    cursor.execute("""
        SELECT
            m.image_id,
            m.filename,
            m.storage_path,
            d.class_name,
            d.confidence,
            d.bbox_x1,
            d.bbox_y1,
            d.bbox_x2,
            d.bbox_y2
        FROM iceberg_dev.cv.image_metadata m
        LEFT JOIN iceberg_dev.cv.detection_results d ON m.image_id = d.image_id
        WHERE m.processed = true
        ORDER BY m.image_id
    """)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    # Group by image
    images = {}
    for row in results:
        image_id, filename, storage_path, class_name, conf, x1, y1, x2, y2 = row
        if image_id not in images:
            images[image_id] = {
                "filename": filename,
                "storage_path": storage_path,
                "detections": []
            }
        if class_name:  # Has detection
            images[image_id]["detections"].append({
                "class_name": class_name,
                "confidence": conf,
                "x1": x1, "y1": y1, "x2": x2, "y2": y2
            })

    return images


def create_preannotation(det, img_width=640, img_height=640):
    """Convert detection to Label Studio annotation format."""
    # Coordinates are already normalized (0-1), convert to percentage (0-100)
    x_pct = det["x1"] * 100
    y_pct = det["y1"] * 100
    w_pct = (det["x2"] - det["x1"]) * 100
    h_pct = (det["y2"] - det["y1"]) * 100

    return {
        "id": f"auto_{det['class_name']}_{int(x_pct)}",
        "type": "rectanglelabels",
        "from_name": "label",
        "to_name": "image",
        "original_width": img_width,
        "original_height": img_height,
        "value": {
            "x": max(0, min(x_pct, 100)),
            "y": max(0, min(y_pct, 100)),
            "width": max(0, min(w_pct, 100 - x_pct)),
            "height": max(0, min(h_pct, 100 - y_pct)),
            "rotation": 0,
            "rectanglelabels": [det["class_name"]]
        },
        "score": det["confidence"]
    }


def create_tasks(project_id, images):
    """Create Label Studio tasks with pre-annotations."""
    headers = get_headers()
    tasks = []

    for image_id, data in images.items():
        filename = data["filename"]
        # Create MinIO URL for the image
        image_url = f"{MINIO_ENDPOINT}/cv-raw/test-images/{filename}"

        # Build pre-annotations
        predictions = []
        if data["detections"]:
            results = [create_preannotation(d) for d in data["detections"]]
            predictions.append({
                "model_version": "yolov8n",
                "score": sum(d["confidence"] for d in data["detections"]) / len(data["detections"]),
                "result": results
            })

        task = {
            "data": {
                "image_url": image_url,
                "image_id": image_id,
            },
            "predictions": predictions
        }
        tasks.append(task)

    # Import tasks
    if tasks:
        resp = requests.post(
            f"{LABELSTUDIO_URL}/api/projects/{project_id}/import",
            headers=headers,
            json=tasks,
            timeout=60,
        )

        if resp.status_code in (200, 201):
            result = resp.json()
            count = result.get("task_count", len(tasks))
            print(f"  Imported {count} tasks with pre-annotations")
            return count
        else:
            print(f"  ERROR: Could not import tasks: {resp.status_code} - {resp.text}")
            return 0

    return 0


def main():
    print("=" * 60)
    print("CVOps Label Studio Setup")
    print("=" * 60)
    print(f"Started: {datetime.now(tz=None).isoformat()}")
    print()

    # Check Label Studio is accessible
    print("Checking Label Studio...")
    try:
        resp = requests.get(f"{LABELSTUDIO_URL}/health", timeout=5)
    except Exception as e:
        # Try dashboard instead
        try:
            resp = requests.get(f"{LABELSTUDIO_URL}/", timeout=5)
            if resp.status_code in (200, 302, 308):
                print(f"  OK: Label Studio accessible")
        except Exception:
            print(f"  ERROR: Label Studio not accessible at {LABELSTUDIO_URL}")
            return

    # Get or create project
    print()
    print("Setting up Label Studio project...")
    project_id = get_or_create_project()
    if not project_id:
        return

    # Get images and detections
    print()
    print("Querying images and detections from Iceberg...")
    try:
        images = get_images_and_detections()
        total_detections = sum(len(img["detections"]) for img in images.values())
        print(f"  Found {len(images)} images with {total_detections} detections")
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    # Create tasks
    print()
    print("Creating Label Studio tasks with pre-annotations...")
    count = create_tasks(project_id, images)

    # Summary
    print()
    print("=" * 60)
    print("Setup Complete")
    print("=" * 60)
    print(f"  Project ID: {project_id}")
    print(f"  Project Name: {PROJECT_NAME}")
    print(f"  Tasks Created: {count}")
    print()
    print("Next Steps:")
    print(f"  1. Open Label Studio: {LABELSTUDIO_URL}")
    print(f"  2. Login (default: admin@admin.com / admin)")
    print(f"  3. Go to project '{PROJECT_NAME}'")
    print(f"  4. Review and correct YOLO pre-annotations")
    print(f"  5. Submit annotations")
    print()


if __name__ == "__main__":
    main()
