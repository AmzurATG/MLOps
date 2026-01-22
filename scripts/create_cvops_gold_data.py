#!/usr/bin/env python3
"""
Create CVOps Gold Training Data
===============================

Creates Gold-standard training data from verified annotations in cv.annotations
and stores it in cv.training_data table with YOLO-compatible format.

Usage:
    python scripts/create_cvops_gold_data.py
"""

import os
import sys
import uuid
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Trino configuration
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = os.getenv("TRINO_PORT", "18083")
TRINO_USER = os.getenv("TRINO_USER", "admin")

# Training split ratios
TRAIN_RATIO = 0.8
VAL_RATIO = 0.2


def get_trino_connection():
    """Get Trino connection."""
    import trino
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=int(TRINO_PORT),
        user=TRINO_USER,
        catalog='iceberg_dev',
        schema='cv'
    )


def get_annotations():
    """Get all verified annotations from cv.annotations."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            a.annotation_id,
            a.image_id,
            m.filename,
            m.storage_path,
            a.class_label,
            a.bbox_x,
            a.bbox_y,
            a.bbox_width,
            a.bbox_height,
            a.source
        FROM iceberg_dev.cv.annotations a
        JOIN iceberg_dev.cv.image_metadata m ON a.image_id = m.image_id
        WHERE a.is_verified = true
    """)

    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return results


def create_class_mapping(annotations):
    """Create class name to ID mapping."""
    class_names = sorted(set(a[4] for a in annotations))  # index 4 is class_label
    return {name: idx for idx, name in enumerate(class_names)}


def assign_splits(image_ids):
    """Assign train/val splits to images."""
    image_list = list(image_ids)
    random.shuffle(image_list)

    n_train = int(len(image_list) * TRAIN_RATIO)
    train_ids = set(image_list[:n_train])
    val_ids = set(image_list[n_train:])

    return train_ids, val_ids


def insert_training_data(annotations, class_map, train_ids, val_ids):
    """Insert training data records into cv.training_data."""
    conn = get_trino_connection()
    cursor = conn.cursor()

    inserted = 0
    by_split = {"train": 0, "val": 0}

    for ann in annotations:
        ann_id, image_id, filename, storage_path, class_label, bbox_x, bbox_y, bbox_w, bbox_h, source = ann

        # Determine split
        if image_id in train_ids:
            split = "train"
        elif image_id in val_ids:
            split = "val"
        else:
            split = "train"  # Default

        # Convert from top-left corner format to YOLO center format
        # YOLO format: x_center, y_center, width, height (all normalized 0-1)
        x_center = bbox_x + (bbox_w / 2)
        y_center = bbox_y + (bbox_h / 2)

        sql = f"""
        INSERT INTO iceberg_dev.cv.training_data (
            image_id, image_path, split, class_label,
            bbox_x_norm, bbox_y_norm, bbox_w_norm, bbox_h_norm,
            annotation_source, created_at
        ) VALUES (
            '{image_id}',
            '{storage_path}',
            '{split}',
            '{class_label}',
            {x_center},
            {y_center},
            {bbox_w},
            {bbox_h},
            '{source}',
            TIMESTAMP '{datetime.now(tz=None).strftime("%Y-%m-%d %H:%M:%S")}'
        )
        """
        try:
            cursor.execute(sql)
            inserted += 1
            by_split[split] += 1
        except Exception as e:
            print(f"    ERROR inserting: {e}")

    cursor.close()
    conn.close()

    return inserted, by_split


def export_yolo_format(annotations, class_map, train_ids, val_ids, output_dir="/tmp/cvops_yolo_dataset"):
    """Export dataset in YOLO format for training."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Create directory structure
    for split in ["train", "val"]:
        (output_path / "images" / split).mkdir(parents=True, exist_ok=True)
        (output_path / "labels" / split).mkdir(parents=True, exist_ok=True)

    # Group annotations by image
    by_image = {}
    for ann in annotations:
        image_id = ann[1]
        if image_id not in by_image:
            by_image[image_id] = {
                "filename": ann[2],
                "storage_path": ann[3],
                "annotations": []
            }
        by_image[image_id]["annotations"].append({
            "class_label": ann[4],
            "bbox_x": ann[5],
            "bbox_y": ann[6],
            "bbox_w": ann[7],
            "bbox_h": ann[8],
        })

    # Write label files
    files_written = 0
    for image_id, data in by_image.items():
        # Determine split
        split = "train" if image_id in train_ids else "val"

        # Label filename (same as image but .txt)
        base_name = Path(data["filename"]).stem
        label_file = output_path / "labels" / split / f"{base_name}.txt"

        # Write YOLO format labels
        with open(label_file, 'w') as f:
            for ann in data["annotations"]:
                class_id = class_map.get(ann["class_label"], 0)
                # YOLO format: class_id x_center y_center width height
                x_center = ann["bbox_x"] + (ann["bbox_w"] / 2)
                y_center = ann["bbox_y"] + (ann["bbox_h"] / 2)
                f.write(f"{class_id} {x_center:.6f} {y_center:.6f} {ann['bbox_w']:.6f} {ann['bbox_h']:.6f}\n")

        files_written += 1

    # Write dataset.yaml
    yaml_content = f"""# CVOps YOLO Dataset
# Generated: {datetime.now(tz=None).isoformat()}

path: {output_path}
train: images/train
val: images/val

# Class names
names:
"""
    for name, idx in sorted(class_map.items(), key=lambda x: x[1]):
        yaml_content += f"  {idx}: {name}\n"

    with open(output_path / "dataset.yaml", 'w') as f:
        f.write(yaml_content)

    return output_path, files_written


def main():
    print("=" * 60)
    print("CVOps Gold Training Data Creation")
    print("=" * 60)
    print(f"Started: {datetime.now(tz=None).isoformat()}")
    print()

    # Get annotations
    print("Fetching verified annotations...")
    annotations = get_annotations()
    print(f"  Found {len(annotations)} verified annotations")

    if not annotations:
        print("  No annotations found. Run auto_accept_cvops_annotations.py first.")
        return

    # Create class mapping
    print()
    print("Creating class mapping...")
    class_map = create_class_mapping(annotations)
    print(f"  Found {len(class_map)} classes:")
    for name, idx in sorted(class_map.items(), key=lambda x: x[1]):
        count = sum(1 for a in annotations if a[4] == name)
        print(f"    {idx}: {name} ({count} annotations)")

    # Get unique images
    image_ids = set(a[1] for a in annotations)
    print(f"\n  Total images: {len(image_ids)}")

    # Assign splits
    print()
    print("Assigning train/val splits...")
    train_ids, val_ids = assign_splits(image_ids)
    print(f"  Train: {len(train_ids)} images")
    print(f"  Val: {len(val_ids)} images")

    # Insert to Iceberg
    print()
    print("Inserting to cv.training_data...")
    inserted, by_split = insert_training_data(annotations, class_map, train_ids, val_ids)
    print(f"  Inserted {inserted} records")
    print(f"    Train: {by_split['train']}")
    print(f"    Val: {by_split['val']}")

    # Export YOLO format
    print()
    print("Exporting YOLO format dataset...")
    output_path, files_written = export_yolo_format(annotations, class_map, train_ids, val_ids)
    print(f"  Exported {files_written} label files")
    print(f"  Dataset path: {output_path}")

    # Verify in Iceberg
    print()
    print("Verifying cv.training_data...")
    conn = get_trino_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT split, COUNT(*), COUNT(DISTINCT image_id), COUNT(DISTINCT class_label)
        FROM iceberg_dev.cv.training_data
        GROUP BY split
    """)
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    if results:
        print("  Split distribution:")
        for split, count, images, classes in results:
            print(f"    {split}: {count} annotations, {images} images, {classes} classes")

    # Summary
    print()
    print("=" * 60)
    print("Gold Training Data Complete")
    print("=" * 60)
    print(f"  Total annotations: {len(annotations)}")
    print(f"  Classes: {len(class_map)}")
    print(f"  Images: {len(image_ids)}")
    print(f"  Train/Val split: {TRAIN_RATIO}/{VAL_RATIO}")
    print(f"  YOLO dataset: {output_path}")
    print()
    print("Ready for model training!")
    print("  Example: yolo train data={output_path}/dataset.yaml model=yolov8n.pt epochs=100")
    print()


if __name__ == "__main__":
    main()
