#!/usr/bin/env python3
"""
Auto-Accept CVOps Annotations
============================

Automatically accepts YOLO pre-annotations in Label Studio and merges them
back to the cv.annotations Iceberg table.

This simulates human review for demo/testing purposes.

Usage:
    python scripts/auto_accept_cvops_annotations.py
"""

import os
import sys
import uuid
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

# Trino configuration
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = os.getenv("TRINO_PORT", "18083")
TRINO_USER = os.getenv("TRINO_USER", "admin")

PROJECT_NAME = "CVOps Object Detection Review"


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
    """Get authenticated headers."""
    access = get_access_token()
    if access:
        return {"Authorization": f"Bearer {access}", "Content-Type": "application/json"}
    return {"Authorization": f"Token {LABELSTUDIO_API_TOKEN}", "Content-Type": "application/json"}


def find_project():
    """Find the CVOps project."""
    headers = get_headers()
    resp = requests.get(f"{LABELSTUDIO_URL}/api/projects/", headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"  ERROR: Could not list projects: {resp.text}")
        return None

    projects = resp.json()
    if isinstance(projects, dict):
        projects = projects.get("results", [])

    for project in projects:
        if project.get("title") == PROJECT_NAME:
            return project["id"]

    return None


def get_tasks_with_predictions(project_id):
    """Get tasks that have predictions but no annotations."""
    headers = get_headers()
    tasks = []
    page = 1

    while True:
        resp = requests.get(
            f"{LABELSTUDIO_URL}/api/projects/{project_id}/tasks",
            headers=headers,
            params={"page": page, "page_size": 100},
            timeout=30,
        )

        if resp.status_code != 200:
            break

        data = resp.json()
        # Handle both list and dict responses
        if isinstance(data, list):
            page_tasks = data
        else:
            page_tasks = data.get("tasks", data.get("results", []))

        if not page_tasks:
            break

        for task in page_tasks:
            # Get task with full predictions
            if task.get("predictions") or task.get("total_predictions", 0) > 0:
                if task.get("total_annotations", 0) == 0:
                    tasks.append(task)

        if len(page_tasks) < 100:
            break
        page += 1

    return tasks


def accept_prediction_as_annotation(task_id, prediction):
    """Convert a prediction to an annotation (accept it)."""
    headers = get_headers()

    annotation = {
        "result": prediction.get("result", []),
        "was_cancelled": False,
        "ground_truth": False,  # Set as human-verified
        "lead_time": 1.0,  # Quick accept
    }

    resp = requests.post(
        f"{LABELSTUDIO_URL}/api/tasks/{task_id}/annotations/",
        headers=headers,
        json=annotation,
        timeout=10,
    )

    return resp.status_code in (200, 201)


def merge_to_iceberg(annotations):
    """Merge accepted annotations to cv.annotations table."""
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
    for ann in annotations:
        image_id = ann["image_id"]

        for bbox in ann["bboxes"]:
            annotation_id = str(uuid.uuid4())

            sql = f"""
            INSERT INTO iceberg_dev.cv.annotations (
                annotation_id, image_id, task_id, annotator, class_label,
                bbox_x, bbox_y, bbox_width, bbox_height, confidence,
                is_verified, annotated_at, source
            ) VALUES (
                '{annotation_id}',
                '{image_id}',
                {ann.get('task_id', 0)},
                'auto_accept',
                '{bbox["class_label"]}',
                {bbox["x"]},
                {bbox["y"]},
                {bbox["width"]},
                {bbox["height"]},
                {bbox.get("confidence", 0.8)},
                true,
                TIMESTAMP '{datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")}',
                'yolo_auto_accepted'
            )
            """
            try:
                cursor.execute(sql)
                inserted += 1
            except Exception as e:
                print(f"    ERROR: {e}")

    cursor.close()
    conn.close()

    return inserted


def main():
    print("=" * 60)
    print("CVOps Auto-Accept Annotations")
    print("=" * 60)
    print(f"Started: {datetime.now(tz=None).isoformat()}")
    print()

    # Find project
    print("Finding Label Studio project...")
    project_id = find_project()
    if not project_id:
        print(f"  ERROR: Project '{PROJECT_NAME}' not found")
        return
    print(f"  Found project: {project_id}")

    # Get tasks with predictions
    print()
    print("Getting tasks with YOLO predictions...")
    tasks = get_tasks_with_predictions(project_id)
    print(f"  Found {len(tasks)} tasks with predictions (no annotations)")

    if not tasks:
        print("  No tasks to process")
        return

    # Accept predictions
    print()
    print("Accepting predictions as annotations...")
    accepted = 0
    annotations_for_iceberg = []

    for task in tasks:
        task_id = task["id"]
        image_id = task.get("data", {}).get("image_id", "")

        # Get full task with predictions
        headers = get_headers()
        resp = requests.get(
            f"{LABELSTUDIO_URL}/api/tasks/{task_id}",
            headers=headers,
            timeout=10,
        )

        if resp.status_code != 200:
            continue

        full_task = resp.json()
        predictions = full_task.get("predictions", [])

        if not predictions:
            continue

        # Use first prediction (YOLO)
        prediction = predictions[0]

        if accept_prediction_as_annotation(task_id, prediction):
            accepted += 1

            # Extract bboxes for Iceberg
            bboxes = []
            for result in prediction.get("result", []):
                if result.get("type") == "rectanglelabels":
                    value = result.get("value", {})
                    labels = value.get("rectanglelabels", [])
                    if labels:
                        bboxes.append({
                            "class_label": labels[0],
                            "x": value.get("x", 0) / 100,  # Convert from % to 0-1
                            "y": value.get("y", 0) / 100,
                            "width": value.get("width", 0) / 100,
                            "height": value.get("height", 0) / 100,
                            "confidence": result.get("score", 0.8),
                        })

            if bboxes:
                annotations_for_iceberg.append({
                    "task_id": task_id,
                    "image_id": image_id,
                    "bboxes": bboxes,
                })

            print(f"    Task {task_id}: {len(bboxes)} annotations accepted")

    print(f"  Accepted {accepted} tasks")

    # Merge to Iceberg
    print()
    print("Merging annotations to Iceberg cv.annotations...")
    inserted = merge_to_iceberg(annotations_for_iceberg)
    print(f"  Inserted {inserted} annotations")

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Tasks processed: {len(tasks)}")
    print(f"  Annotations accepted: {accepted}")
    print(f"  Records inserted to Iceberg: {inserted}")
    print()
    print("Next Steps:")
    print("  1. Run scripts/create_cvops_gold_data.py to create training data")
    print()


if __name__ == "__main__":
    main()
