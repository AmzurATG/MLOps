"""
Schedules and Sensors for Automated Triggers
Cron-based scheduling and event-driven sensors.
"""
import os
import time
import requests
from dagster import (
    ScheduleDefinition,
    sensor,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    DefaultScheduleStatus,
    DefaultSensorStatus,
)


# =============================================================================
# SCHEDULE FACTORIES
# =============================================================================

def create_daily_schedule(job, name: str, hour: int = 6):
    """Create daily schedule at specified hour UTC."""
    return ScheduleDefinition(
        job=job,
        cron_schedule=f"0 {hour} * * *",
        name=name,
        description=f"Daily at {hour}:00 UTC",
        default_status=DefaultScheduleStatus.STOPPED,
        execution_timezone="UTC",
    )


def create_hourly_schedule(job, name: str):
    """Create hourly schedule."""
    return ScheduleDefinition(
        job=job,
        cron_schedule="0 * * * *",
        name=name,
        description="Every hour",
        default_status=DefaultScheduleStatus.STOPPED,
        execution_timezone="UTC",
    )


# =============================================================================
# MLOPS SENSORS
# =============================================================================

def create_new_annotations_sensor(job):
    """Trigger job when new annotations detected in Label Studio."""
    
    @sensor(
        job=job,
        minimum_interval_seconds=300,  # 5 min
        default_status=DefaultSensorStatus.STOPPED,
        name="new_annotations_sensor",
        description="Triggers when new annotations are completed",
    )
    def _sensor(context: SensorEvaluationContext):
        last_count = int(context.cursor or "0")
        
        ls_url = os.getenv("LABELSTUDIO_URL", "http://exp-label-studio:8080")
        api_token = os.getenv("LABELSTUDIO_API_TOKEN", "")
        username = os.getenv("LABELSTUDIO_USERNAME", "")
        password = os.getenv("LABELSTUDIO_PASSWORD", "")
        project_id = os.getenv("LABELSTUDIO_PROJECT_ID", "1")
        
        # Get auth headers
        headers = None
        if api_token:
            try:
                r = requests.post(f"{ls_url}/api/token/refresh", json={"refresh": api_token}, timeout=10)
                if r.ok and r.json().get("access"):
                    headers = {"Authorization": f"Bearer {r.json()['access']}"}
            except:
                pass
        
        if not headers and username and password:
            try:
                r = requests.post(f"{ls_url}/api/auth/login", json={"username": username, "password": password}, timeout=10)
                if r.ok:
                    headers = {"Authorization": f"Token {r.json()['token']}"}
            except:
                pass
        
        if not headers:
            return SkipReason("Auth failed: set LABELSTUDIO_API_TOKEN or USERNAME+PASSWORD")
        
        try:
            resp = requests.get(f"{ls_url}/api/projects/{project_id}/", headers=headers, timeout=10)
            if not resp.ok:
                return SkipReason(f"API error: {resp.status_code}")
            
            current_count = resp.json().get("num_tasks_with_annotations", 0)
            
            if current_count > last_count:
                context.update_cursor(str(current_count))
                return RunRequest(
                    run_key=f"annotations_{current_count}",
                    tags={"trigger": "new_annotations"},
                )
            else:
                return SkipReason(f"No new annotations (count: {current_count})")
        
        except Exception as e:
            return SkipReason(f"Error: {e}")
    
    return _sensor


def create_stale_data_sensor(job, max_hours: int = 24):
    """Trigger job when source data hasn't been synced."""
    
    @sensor(
        job=job,
        minimum_interval_seconds=3600,  # 1 hour
        default_status=DefaultSensorStatus.STOPPED,
        name="stale_data_sensor",
        description=f"Triggers sync when data is older than {max_hours}h",
    )
    def _sensor(context: SensorEvaluationContext):
        last_sync = int(context.cursor or "0")
        current_time = int(time.time())
        
        hours_since = (current_time - last_sync) / 3600
        
        if hours_since >= max_hours:
            context.update_cursor(str(current_time))
            return RunRequest(
                run_key=f"stale_{current_time}",
                tags={"trigger": "stale_data", "hours_since": str(hours_since)},
            )
        else:
            return SkipReason(f"Last sync {hours_since:.1f}h ago (waiting for {max_hours}h)")
    
    return _sensor


# =============================================================================
# CVOPS SENSORS
# =============================================================================

def create_new_images_sensor(job):
    """Trigger detection when new images are uploaded."""
    
    @sensor(
        job=job,
        minimum_interval_seconds=600,  # 10 min
        default_status=DefaultSensorStatus.STOPPED,
        name="new_images_sensor",
        description="Triggers when new images are found in storage",
    )
    def _sensor(context: SensorEvaluationContext):
        last_count = int(context.cursor or "0")
        
        try:
            import boto3
            
            client = boto3.client(
                "s3",
                endpoint_url=os.getenv("MINIO_ENDPOINT", "http://exp-minio:9000"),
                aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
                aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
            )
            
            bucket = os.getenv("CVOPS_RAW_BUCKET", "cv-raw")
            
            # Count images
            paginator = client.get_paginator("list_objects_v2")
            current_count = 0
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    if obj["Key"].lower().endswith((".jpg", ".jpeg", ".png")):
                        current_count += 1
            
            if current_count > last_count:
                new_count = current_count - last_count
                context.update_cursor(str(current_count))
                return RunRequest(
                    run_key=f"images_{current_count}",
                    tags={"trigger": "new_images", "new_count": str(new_count)},
                )
            else:
                return SkipReason(f"No new images (count: {current_count})")
        
        except Exception as e:
            return SkipReason(f"Error: {e}")
    
    return _sensor