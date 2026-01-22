"""
CV Detection API
================

Production-grade object detection API following fraud detection API patterns.

Features:
- Model loading from MLflow by stage/run_id
- Single and batch detection endpoints
- Write-back to Kafka and Iceberg
- Prometheus metrics
- Health check and model info endpoints

Endpoints:
    POST /detect           - Single image detection
    POST /detect/batch     - Batch detection
    POST /detect/base64    - Base64 encoded image
    GET  /models           - List available models
    GET  /health           - Health check
    GET  /metrics          - Prometheus metrics
"""
import os
import sys
import time
import uuid
import base64
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from io import BytesIO

from fastapi import FastAPI, HTTPException, Query, File, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field
import numpy as np

# Add app to path
sys.path.insert(0, "/app")


# =============================================================================
# CONFIGURATION
# =============================================================================

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://exp-mlflow:5000")
MODEL_NAME = os.getenv("CV_MODEL_NAME", "yolo-object-detector")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "exp-kafka:9092")
TRINO_HOST = os.getenv("TRINO_HOST", "exp-trino")
WRITE_BACK_ENABLED = os.getenv("CV_WRITE_BACK", "true").lower() in ("true", "1", "yes")
YOLO_DEVICE = os.getenv("YOLO_DEVICE", "cpu")
YOLO_CONFIDENCE = float(os.getenv("YOLO_CONFIDENCE", "0.25"))

# OPTIMIZED: Kafka producer singleton (avoid creating connection per message)
_kafka_producer = None


def get_kafka_producer():
    """
    Get or create Kafka producer singleton.

    OPTIMIZED: Reuses single producer instance instead of creating
    new connection for every message. Includes delivery callback.
    """
    global _kafka_producer
    if _kafka_producer is None and WRITE_BACK_ENABLED:
        try:
            from confluent_kafka import Producer
            _kafka_producer = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "linger.ms": 5,  # Small batch window for better throughput
                "batch.size": 16384,
                "acks": 1,  # Faster than "all" for non-critical writes
            })
            print(f"[CV-API] Kafka producer initialized: {KAFKA_BOOTSTRAP}")
        except Exception as e:
            print(f"[CV-API] Kafka producer init failed: {e}")
    return _kafka_producer


# =============================================================================
# PROMETHEUS METRICS
# =============================================================================

try:
    from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
    
    CV_REQUESTS = Counter(
        "cv_detection_requests_total",
        "Total detection requests",
        ["status", "model_stage"]
    )
    CV_LATENCY = Histogram(
        "cv_detection_latency_seconds",
        "Detection latency",
        ["model_stage"],
        buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
    )
    CV_OBJECTS = Counter(
        "cv_objects_detected_total",
        "Objects detected by class",
        ["class_name", "model_stage"]
    )
    CV_MODEL_LOADED = Gauge(
        "cv_model_loaded",
        "Model loaded indicator",
        ["model_stage", "version"]
    )
    
    PROMETHEUS_ENABLED = True
except ImportError:
    PROMETHEUS_ENABLED = False
    print("[CV-API] Prometheus not available")


def track_detection(status: str, model_stage: str, latency: float, detections: List[Dict]):
    """Track detection metrics."""
    if not PROMETHEUS_ENABLED:
        return
    
    CV_REQUESTS.labels(status=status, model_stage=model_stage).inc()
    CV_LATENCY.labels(model_stage=model_stage).observe(latency)
    
    for det in detections:
        CV_OBJECTS.labels(
            class_name=det.get("class_name", "unknown"),
            model_stage=model_stage
        ).inc()


# =============================================================================
# MODEL MANAGEMENT
# =============================================================================

_model_cache: Dict[str, Any] = {}


def get_mlflow_client():
    """Get MLflow client."""
    import mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    return mlflow.tracking.MlflowClient()


def load_model(
    model_stage: Optional[str] = None,
    run_id: Optional[str] = None,
) -> Tuple[Any, Dict[str, str]]:
    """
    Load YOLO model from MLflow or local.
    
    Returns:
        (model, model_info_dict)
    """
    global _model_cache
    
    from ultralytics import YOLO
    
    # Build cache key
    if run_id:
        cache_key = f"run_{run_id}"
    elif model_stage:
        cache_key = f"stage_{model_stage}"
    else:
        cache_key = "stage_Production"
        model_stage = "Production"
    
    # Check cache
    if cache_key in _model_cache:
        return _model_cache[cache_key]
    
    print(f"[CV-API] Loading model: {cache_key}", flush=True)
    
    model_info = {
        "model_name": MODEL_NAME,
        "stage": model_stage or "local",
    }
    
    try:
        import mlflow
        client = get_mlflow_client()
        
        if run_id:
            model_path = mlflow.artifacts.download_artifacts(
                run_id=run_id,
                artifact_path="model/best.pt",
            )
            model_info["run_id"] = run_id
            model_info["source"] = "mlflow_run"
        else:
            versions = client.get_latest_versions(MODEL_NAME, stages=[model_stage])
            if not versions:
                # Fall back to local model
                raise ValueError(f"No model in stage {model_stage}")
            
            run_id = versions[0].run_id
            model_path = mlflow.artifacts.download_artifacts(
                run_id=run_id,
                artifact_path="model/best.pt",
            )
            model_info["run_id"] = run_id
            model_info["version"] = versions[0].version
            model_info["source"] = "mlflow_registry"
        
        model = YOLO(model_path)
        model.to(YOLO_DEVICE)
        model_info["device"] = YOLO_DEVICE
        
    except Exception as e:
        print(f"[CV-API] MLflow load failed: {e}, using default model", flush=True)
        model = YOLO(os.getenv("YOLO_MODEL_PATH", "yolov8n.pt"))
        model.to(YOLO_DEVICE)
        model_info["source"] = "local"
        model_info["device"] = YOLO_DEVICE
    
    _model_cache[cache_key] = (model, model_info)
    
    if PROMETHEUS_ENABLED:
        CV_MODEL_LOADED.labels(
            model_stage=model_stage or "local",
            version=model_info.get("version", "unknown")
        ).set(1)
    
    print(f"[CV-API] ✅ Loaded model: {cache_key}", flush=True)
    return model, model_info


# =============================================================================
# WRITE-BACK
# =============================================================================

def publish_detection_result(
    image_id: str,
    detections: List[Dict],
    model_info: Dict[str, str],
    inference_ms: float,
):
    """
    Publish detection result to Kafka.

    OPTIMIZED: Uses singleton producer to avoid connection overhead per message.
    """
    if not WRITE_BACK_ENABLED:
        return

    producer = get_kafka_producer()
    if producer is None:
        return

    try:
        import json

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
            "cv.detections.results",
            key=image_id.encode(),
            value=json.dumps(message).encode(),
        )
        # Don't flush every message - let batching work
        # Producer will auto-flush based on linger.ms and batch.size
        producer.poll(0)  # Trigger delivery callbacks without blocking

    except Exception as e:
        print(f"[CV-API] Write-back failed: {e}")


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class Detection(BaseModel):
    """Single detection result."""
    class_name: str
    class_id: int
    confidence: float
    bbox_x: float
    bbox_y: float
    bbox_width: float
    bbox_height: float


class DetectionResponse(BaseModel):
    """Detection API response."""
    image_id: str
    detections: List[Detection]
    num_objects: int
    inference_latency_ms: float
    model_name: str
    model_version: str
    model_stage: str
    timestamp: str


class Base64DetectionRequest(BaseModel):
    """Request for base64 detection."""
    image_data: str = Field(..., description="Base64 encoded image")
    image_id: Optional[str] = None


# =============================================================================
# FASTAPI APP
# =============================================================================

app = FastAPI(
    title="CV Detection API",
    description="Object detection inference with YOLO",
    version="1.0.0",
)


@app.on_event("startup")
async def startup():
    """Initialize on startup."""
    print("[CV-API] Started")


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.post("/detect", response_model=DetectionResponse)
async def detect_objects(
    file: UploadFile = File(...),
    model_stage: str = Query("Production", description="Model stage"),
    run_id: Optional[str] = Query(None, description="Specific run ID"),
    confidence_threshold: float = Query(0.25, ge=0, le=1),
    image_id: Optional[str] = Query(None),
):
    """
    Detect objects in uploaded image.
    
    Results are automatically written back to Kafka.
    """
    import cv2
    
    start_time = time.time()
    
    try:
        # Load image
        contents = await file.read()
        nparr = np.frombuffer(contents, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            track_detection("error", model_stage, 0, [])
            raise HTTPException(status_code=400, detail="Invalid image")
        
        # Load model
        model, model_info = load_model(model_stage, run_id)
        
        # Run inference
        inference_start = time.time()
        results = model(image, conf=confidence_threshold, verbose=False)[0]
        inference_ms = (time.time() - inference_start) * 1000
        
        # Parse detections
        detections = []
        img_h, img_w = image.shape[:2]
        
        for box in results.boxes:
            class_id = int(box.cls[0])
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            
            det = Detection(
                class_name=model.names[class_id],
                class_id=class_id,
                confidence=float(box.conf[0]),
                bbox_x=x1 / img_w,
                bbox_y=y1 / img_h,
                bbox_width=(x2 - x1) / img_w,
                bbox_height=(y2 - y1) / img_h,
            )
            detections.append(det)
        
        total_latency = time.time() - start_time
        
        # Track metrics
        track_detection("success", model_stage, total_latency, [d.dict() for d in detections])
        
        # Write-back
        final_image_id = image_id or file.filename or str(uuid.uuid4())
        publish_detection_result(
            image_id=final_image_id,
            detections=[d.dict() for d in detections],
            model_info=model_info,
            inference_ms=inference_ms,
        )
        
        return DetectionResponse(
            image_id=final_image_id,
            detections=detections,
            num_objects=len(detections),
            inference_latency_ms=round(inference_ms, 2),
            model_name=model_info.get("model_name", MODEL_NAME),
            model_version=model_info.get("version", "unknown"),
            model_stage=model_stage,
            timestamp=datetime.utcnow().isoformat(),
        )
        
    except HTTPException:
        raise
    except Exception as e:
        track_detection("error", model_stage, time.time() - start_time, [])
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/detect/base64", response_model=DetectionResponse)
async def detect_base64(
    request: Base64DetectionRequest,
    model_stage: str = Query("Production"),
    confidence_threshold: float = Query(0.25),
):
    """Detect objects in base64-encoded image."""
    import cv2
    
    start_time = time.time()
    
    try:
        # Decode base64
        image_bytes = base64.b64decode(request.image_data)
        nparr = np.frombuffer(image_bytes, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        if image is None:
            raise HTTPException(status_code=400, detail="Invalid image")
        
        # Load model
        model, model_info = load_model(model_stage)
        
        # Run inference
        inference_start = time.time()
        results = model(image, conf=confidence_threshold, verbose=False)[0]
        inference_ms = (time.time() - inference_start) * 1000
        
        # Parse detections
        detections = []
        img_h, img_w = image.shape[:2]
        
        for box in results.boxes:
            class_id = int(box.cls[0])
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            
            detections.append(Detection(
                class_name=model.names[class_id],
                class_id=class_id,
                confidence=float(box.conf[0]),
                bbox_x=x1 / img_w,
                bbox_y=y1 / img_h,
                bbox_width=(x2 - x1) / img_w,
                bbox_height=(y2 - y1) / img_h,
            ))
        
        # Track and publish
        image_id = request.image_id or str(uuid.uuid4())
        track_detection("success", model_stage, time.time() - start_time, [d.dict() for d in detections])
        publish_detection_result(image_id, [d.dict() for d in detections], model_info, inference_ms)
        
        return DetectionResponse(
            image_id=image_id,
            detections=detections,
            num_objects=len(detections),
            inference_latency_ms=round(inference_ms, 2),
            model_name=model_info.get("model_name", MODEL_NAME),
            model_version=model_info.get("version", "unknown"),
            model_stage=model_stage,
            timestamp=datetime.utcnow().isoformat(),
        )
        
    except HTTPException:
        raise
    except Exception as e:
        track_detection("error", model_stage, time.time() - start_time, [])
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/detect/batch")
async def detect_batch(
    files: List[UploadFile] = File(...),
    model_stage: str = Query("Production"),
    confidence_threshold: float = Query(0.25),
) -> List[DetectionResponse]:
    """Batch detection for multiple images."""
    results = []
    for file in files:
        result = await detect_objects(
            file=file,
            model_stage=model_stage,
            confidence_threshold=confidence_threshold,
        )
        results.append(result)
    return results


@app.get("/models")
def list_models():
    """List available models from MLflow."""
    try:
        client = get_mlflow_client()
        
        models = []
        for stage in ["Production", "Staging", "None"]:
            try:
                versions = client.get_latest_versions(MODEL_NAME, stages=[stage])
                for v in versions:
                    models.append({
                        "name": MODEL_NAME,
                        "version": v.version,
                        "stage": v.current_stage,
                        "run_id": v.run_id,
                        "status": v.status,
                    })
            except Exception:
                pass
        
        return {"models": models}
    except Exception as e:
        return {"models": [], "error": str(e)}


@app.get("/health")
def health():
    """Health check."""
    return {
        "status": "healthy",
        "models_cached": len(_model_cache),
        "write_back_enabled": WRITE_BACK_ENABLED,
        "device": YOLO_DEVICE,
    }


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    if PROMETHEUS_ENABLED:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    return {"error": "Prometheus not available"}


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
