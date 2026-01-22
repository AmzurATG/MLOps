#!/usr/bin/env python
"""
Auto-Annotate Label Studio Tasks (Demo Mode)

Automatically annotates all pending tasks in Label Studio using the source
`is_fraudulent` value. Preserves the Label Studio flow for demos while
skipping manual annotation.

USE CASES:
  1. Demo mode: Run full pipeline without manual labeling
  2. Bulk approval: Accept source labels as ground truth
  3. Pre-annotation: Initialize labels before human review

USAGE:
  python auto_annotate_labelstudio.py                    # Annotate all pending
  python auto_annotate_labelstudio.py --dry-run         # Preview only
  python auto_annotate_labelstudio.py --limit 100       # First 100 tasks
"""

import os
import sys
import argparse
from typing import Dict, List, Optional, Any, Tuple

import requests

# =============================================================================
# CONFIGURATION
# =============================================================================

LABELSTUDIO_URL = os.getenv(
    "LABELSTUDIO_EXTERNAL_URL",
    os.getenv("LABELSTUDIO_URL", "http://localhost:18081")
)
LABELSTUDIO_API_TOKEN = os.getenv("LABELSTUDIO_API_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoicmVmcmVzaCIsImV4cCI6ODA3NTYwNTM5NCwiaWF0IjoxNzY4NDA1Mzk0LCJqdGkiOiJiNGVjODQwY2MzMjE0YjJmOWY0M2RmMWRlYTJmOTg0ZiIsInVzZXJfaWQiOjF9.y7tijAMLaDMoc2Q1Oi2m4_HngHilbAPiCRQ1JSugLp0")
PROJECT_NAME = "Fraud Detection Review"
SOURCE_LABEL_FIELD = "is_fraudulent"
ANNOTATION_FROM_NAME = "fraud_label"
ANNOTATION_TO_NAME = "tx_info"


def normalize_label(value: Any) -> Optional[str]:
    """Convert any label value to 'fraud' or 'legit'."""
    if value is None:
        return None
    str_val = str(value).lower().strip()
    if str_val in ("1", "true", "fraud"):
        return "fraud"
    if str_val in ("0", "false", "legit", "legitimate"):
        return "legit"
    return None


class LabelStudioClient:
    """Simple Label Studio API client with auto-auth."""

    def __init__(self, url: str, token: str):
        self.url = url.rstrip("/")
        self.token = token
        self._access_token = None
        self.headers = self._init_auth()

    def _init_auth(self) -> Dict[str, str]:
        """Initialize authentication - try JWT refresh first, fallback to Token."""
        # Try JWT refresh
        try:
            resp = requests.post(
                f"{self.url}/api/token/refresh",
                json={"refresh": self.token},
                timeout=10,
            )
            if resp.ok and resp.json().get("access"):
                self._access_token = resp.json()["access"]
                print("Using JWT authentication")
                return {"Authorization": f"Bearer {self._access_token}"}
        except Exception:
            pass

        # Fallback to Token auth
        print("Using Token authentication")
        return {"Authorization": f"Token {self.token}"}

    def _refresh_token(self) -> bool:
        """Refresh JWT token if applicable."""
        if not self._access_token:
            return False
        try:
            resp = requests.post(
                f"{self.url}/api/token/refresh",
                json={"refresh": self.token},
                timeout=10,
            )
            if resp.ok:
                self._access_token = resp.json().get("access")
                self.headers = {"Authorization": f"Bearer {self._access_token}"}
                return True
        except Exception:
            pass
        return False

    def get(self, path: str, **kwargs) -> requests.Response:
        """GET request with auth."""
        return requests.get(f"{self.url}{path}", headers=self.headers, **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        """POST request with auth and retry on 401."""
        resp = requests.post(f"{self.url}{path}", headers=self.headers, **kwargs)
        if resp.status_code == 401 and self._refresh_token():
            resp = requests.post(f"{self.url}{path}", headers=self.headers, **kwargs)
        return resp


class LabelStudioAutoAnnotator:
    """Auto-annotate Label Studio tasks using source labels."""

    def __init__(self, url: str = None, token: str = None, project_name: str = None):
        self.client = LabelStudioClient(url or LABELSTUDIO_URL, token or LABELSTUDIO_API_TOKEN)
        self.project_name = project_name or PROJECT_NAME
        self.project_id = self._find_project()

    def _find_project(self) -> int:
        """Find project by name."""
        resp = self.client.get("/api/projects/", timeout=10)
        resp.raise_for_status()

        data = resp.json()
        projects = data.get("results", data) if isinstance(data, dict) else data

        matching = [p for p in projects if p.get("title") == self.project_name]
        if not matching:
            available = [p.get("title") for p in projects]
            raise ValueError(f"Project '{self.project_name}' not found. Available: {available}")

        # Use project with most tasks if duplicates
        project = max(matching, key=lambda p: p.get("task_number", 0))
        print(f"Connected to '{project['title']}' (ID: {project['id']}, Tasks: {project.get('task_number', 0)})")
        return project["id"]

    def get_pending_tasks(self, limit: Optional[int] = None) -> List[Dict]:
        """Get unannotated tasks with pagination."""
        tasks = []
        page = 1

        while True:
            resp = self.client.get(
                f"/api/projects/{self.project_id}/tasks",
                params={"page": page, "page_size": 100},
                timeout=30,
            )
            if resp.status_code == 404:
                break
            resp.raise_for_status()

            data = resp.json()
            page_tasks = data.get("tasks", data.get("results", data)) if isinstance(data, dict) else data

            if not page_tasks:
                break

            for task in page_tasks:
                if task.get("total_annotations", 0) == 0:
                    tasks.append(task)
                    if limit and len(tasks) >= limit:
                        return tasks

            # Check for more pages
            if isinstance(data, dict) and not data.get("next"):
                break
            if len(page_tasks) < 100:
                break
            page += 1

        return tasks

    def get_source_label(self, task: Dict) -> Optional[str]:
        """Extract and normalize source label from task data."""
        data = task.get("data", {})

        # Check common field names
        for field in [SOURCE_LABEL_FIELD, "is_fraud", "label", "fraud_label"]:
            if field in data:
                return normalize_label(data[field])

        # Check nested structures
        for value in data.values():
            if isinstance(value, dict) and SOURCE_LABEL_FIELD in value:
                return normalize_label(value[SOURCE_LABEL_FIELD])

        return None

    def create_annotation(self, task_id: int, label: str) -> Dict:
        """Create annotation for a task."""
        annotation = {
            "result": [{
                "type": "choices",
                "from_name": ANNOTATION_FROM_NAME,
                "to_name": ANNOTATION_TO_NAME,
                "value": {"choices": [label]}
            }],
            "was_cancelled": False,
            "ground_truth": True,
        }
        resp = self.client.post(f"/api/tasks/{task_id}/annotations/", json=annotation, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def auto_annotate(self, limit: Optional[int] = None, dry_run: bool = False, verbose: bool = True) -> Dict[str, Any]:
        """Auto-annotate all pending tasks."""
        print(f"\n{'=' * 50}")
        print(f"AUTO-ANNOTATE: {'DRY RUN' if dry_run else 'LIVE'}")
        print(f"{'=' * 50}")

        tasks = self.get_pending_tasks(limit)
        print(f"Found {len(tasks)} unannotated tasks")

        if not tasks:
            return {"total": 0, "annotated": 0, "skipped": 0, "errors": 0}

        stats = {"total": len(tasks), "annotated": 0, "skipped": 0, "errors": 0, "by_label": {}}

        for i, task in enumerate(tasks, 1):
            task_id = task["id"]
            label = self.get_source_label(task)

            if not label:
                stats["skipped"] += 1
                if verbose:
                    print(f"  [{i}] Task {task_id}: No label, skipping")
                continue

            if verbose and (i <= 10 or i % 500 == 0):
                tx_id = task.get("data", {}).get("transaction_id", "?")
                print(f"  [{i}/{len(tasks)}] Task {task_id} ({tx_id}) -> {label}")

            if not dry_run:
                try:
                    self.create_annotation(task_id, label)
                    stats["annotated"] += 1
                    stats["by_label"][label] = stats["by_label"].get(label, 0) + 1
                except Exception as e:
                    print(f"  ERROR Task {task_id}: {e}")
                    stats["errors"] += 1
            else:
                stats["annotated"] += 1
                stats["by_label"][label] = stats["by_label"].get(label, 0) + 1

        # Summary
        print(f"\n{'=' * 50}")
        print(f"SUMMARY: {stats['annotated']} annotated, {stats['skipped']} skipped, {stats['errors']} errors")
        for label, count in stats["by_label"].items():
            pct = count / max(stats["annotated"], 1) * 100
            print(f"  {label}: {count} ({pct:.1f}%)")

        if dry_run:
            print("\nDRY RUN - No changes made.")
        return stats


def main():
    parser = argparse.ArgumentParser(description="Auto-annotate Label Studio tasks")
    parser.add_argument("--project", "-p", type=int, help="Project ID")
    parser.add_argument("--project-name", default=PROJECT_NAME, help="Project name")
    parser.add_argument("--url", default=LABELSTUDIO_URL, help="Label Studio URL")
    parser.add_argument("--token", default=LABELSTUDIO_API_TOKEN, help="API token")
    parser.add_argument("--limit", "-l", type=int, help="Max tasks to annotate")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--quiet", "-q", action="store_true", help="Less verbose")

    args = parser.parse_args()

    try:
        annotator = LabelStudioAutoAnnotator(
            url=args.url,
            token=args.token,
            project_name=args.project_name,
        )
        stats = annotator.auto_annotate(
            limit=args.limit,
            dry_run=args.dry_run,
            verbose=not args.quiet,
        )
        sys.exit(1 if stats["errors"] > 0 else 0)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
