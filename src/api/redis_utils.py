"""
Redis Utility Module
====================

Provides safe Redis operations that handle both:
1. Feast Online Store keys (can be binary with v3, string with v2)
2. Streaming feature keys (always string-based)

Key Patterns:
- Feast (v2): "feast:online:{feature_view}:{entity_key}:{entity_value}"
- Feast (v3): Binary protobuf format (not human-readable)
- Streaming: "feast:streaming:{customer_id}"

Usage:
    from src.api.redis_utils import get_redis_client, inspect_redis_keys, get_all_customer_features

Configuration:
    FEAST_KEY_SERIALIZATION_VERSION in feature_store.yaml determines key format:
    - Version 2: Human-readable string keys (recommended for debugging)
    - Version 3: Binary protobuf keys (more compact, harder to debug)
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "exp-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FEAST_ONLINE_PREFIX = os.getenv("FEAST_ONLINE_PREFIX", "feast:online:")
STREAMING_PREFIX = os.getenv("REDIS_STREAMING_PREFIX", "feast:streaming:")
REDIS_POOL_MAX_CONNECTIONS = int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "50"))

# Global connection pools - created lazily
_binary_pool = None
_text_pool = None


@dataclass
class RedisKeyInfo:
    """Information about a Redis key."""
    key: str
    key_type: str  # 'feast', 'streaming', 'unknown'
    readable: bool  # Whether the key is human-readable
    ttl: int  # Time-to-live in seconds (-1 = no expiry, -2 = key doesn't exist)
    size: int  # Approximate memory size in bytes


def _get_pool(decode_responses: bool = False):
    """
    Get or create a connection pool.

    OPTIMIZED: Uses global connection pools to avoid creating new connections
    for every operation. This dramatically reduces connection overhead.
    """
    import redis
    global _binary_pool, _text_pool

    if decode_responses:
        if _text_pool is None:
            _text_pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                max_connections=REDIS_POOL_MAX_CONNECTIONS,
                decode_responses=True,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            logger.info(f"Created text Redis pool (max_connections={REDIS_POOL_MAX_CONNECTIONS})")
        return _text_pool
    else:
        if _binary_pool is None:
            _binary_pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                max_connections=REDIS_POOL_MAX_CONNECTIONS,
                decode_responses=False,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            logger.info(f"Created binary Redis pool (max_connections={REDIS_POOL_MAX_CONNECTIONS})")
        return _binary_pool


def get_redis_client(decode_responses: bool = False):
    """
    Get a Redis client with connection pooling.

    OPTIMIZED: Uses shared connection pool instead of creating new connections.

    Args:
        decode_responses: If True, decode bytes to strings.
                         Use False for binary Feast keys (v3).
                         Use True for streaming keys and Feast v2 keys.

    Returns:
        redis.Redis client instance (from pool)
    """
    import redis
    return redis.Redis(connection_pool=_get_pool(decode_responses))


def get_streaming_client():
    """Get a Redis client configured for streaming features (text mode)."""
    return get_redis_client(decode_responses=True)


def get_feast_client():
    """
    Get a Redis client configured for Feast features.

    With entity_key_serialization_version: 2, keys are strings.
    With version 3, keys are binary.
    """
    # Default to binary mode for compatibility with both versions
    return get_redis_client(decode_responses=False)


def inspect_redis_keys(
    pattern: str = "*",
    limit: int = 100,
    include_values: bool = False,
) -> Dict[str, Any]:
    """
    Inspect Redis keys matching a pattern.

    Safely handles both binary and text keys.

    Args:
        pattern: Redis key pattern (e.g., "feast:*", "*customer*")
        limit: Maximum number of keys to return
        include_values: If True, include key values (may be large)

    Returns:
        Dict with key statistics and sample keys
    """
    # Use binary client to see all keys
    r = get_redis_client(decode_responses=False)

    result = {
        "pattern": pattern,
        "total_keys": 0,
        "feast_online_keys": 0,
        "streaming_keys": 0,
        "binary_keys": 0,
        "unknown_keys": 0,
        "sample_keys": [],
    }

    try:
        cursor = 0
        keys_found = []
        pattern_bytes = pattern.encode() if isinstance(pattern, str) else pattern

        while len(keys_found) < limit:
            cursor, keys = r.scan(cursor, match=pattern_bytes, count=100)
            keys_found.extend(keys)
            if cursor == 0:
                break

        result["total_keys"] = len(keys_found)

        for key in keys_found[:limit]:
            key_info = _analyze_key(r, key)

            if key_info.key_type == "feast":
                result["feast_online_keys"] += 1
            elif key_info.key_type == "streaming":
                result["streaming_keys"] += 1
            elif not key_info.readable:
                result["binary_keys"] += 1
            else:
                result["unknown_keys"] += 1

            sample = {
                "key": key_info.key,
                "type": key_info.key_type,
                "readable": key_info.readable,
                "ttl": key_info.ttl,
            }

            if include_values and key_info.readable:
                try:
                    key_type = r.type(key).decode()
                    if key_type == "hash":
                        # For streaming features (hash type)
                        r_text = get_redis_client(decode_responses=True)
                        sample["value"] = r_text.hgetall(key_info.key)
                    elif key_type == "string":
                        sample["value"] = r.get(key).decode("utf-8", errors="replace")
                except Exception as e:
                    sample["value_error"] = str(e)[:50]

            result["sample_keys"].append(sample)

    except Exception as e:
        result["error"] = str(e)
    # Note: No .close() needed - connection pool handles lifecycle

    return result


def _analyze_key(r, key_bytes: bytes) -> RedisKeyInfo:
    """Analyze a single Redis key."""
    # Try to decode as string
    try:
        key_str = key_bytes.decode("utf-8")
        readable = True
    except UnicodeDecodeError:
        # Binary key (likely Feast v3 format)
        key_str = f"<binary:{len(key_bytes)}bytes>"
        readable = False

    # Determine key type
    if readable:
        if key_str.startswith(STREAMING_PREFIX):
            key_type = "streaming"
        elif key_str.startswith(FEAST_ONLINE_PREFIX) or key_str.startswith("feast:"):
            key_type = "feast"
        else:
            key_type = "unknown"
    else:
        # Binary keys are typically Feast v3 format
        key_type = "feast"

    # Get TTL
    try:
        ttl = r.ttl(key_bytes)
    except Exception:
        ttl = -2

    # Get approximate memory size
    try:
        size = r.memory_usage(key_bytes) or 0
    except Exception:
        size = 0

    return RedisKeyInfo(
        key=key_str,
        key_type=key_type,
        readable=readable,
        ttl=ttl,
        size=size,
    )


def get_streaming_features(customer_id: str) -> Tuple[Dict[str, Any], bool]:
    """
    Get streaming features for a customer.

    OPTIMIZED: Uses connection pool - no connection overhead per call.

    Args:
        customer_id: The customer identifier

    Returns:
        Tuple of (features_dict, success)
    """
    r = get_streaming_client()
    try:
        key = f"{STREAMING_PREFIX}{customer_id}"
        features = r.hgetall(key)
        return features, bool(features)
    except Exception as e:
        logger.error(f"Error fetching streaming features: {e}")
        return {}, False
    # No .close() needed - connection pool handles lifecycle


def set_streaming_features(
    customer_id: str,
    features: Dict[str, Any],
    ttl_seconds: int = 3600,
) -> bool:
    """
    Set streaming features for a customer.

    OPTIMIZED: Uses connection pool and pipelined operations.

    Args:
        customer_id: The customer identifier
        features: Dict of feature name -> value
        ttl_seconds: Time-to-live in seconds (default 1 hour)

    Returns:
        True if successful
    """
    r = get_streaming_client()
    try:
        key = f"{STREAMING_PREFIX}{customer_id}"
        # Convert all values to strings for Redis
        str_features = {k: str(v) for k, v in features.items()}
        # Use pipeline for atomic set + expire
        pipe = r.pipeline(transaction=False)
        pipe.hset(key, mapping=str_features)
        if ttl_seconds > 0:
            pipe.expire(key, ttl_seconds)
        pipe.execute()
        return True
    except Exception as e:
        logger.error(f"Error setting streaming features: {e}")
        return False
    # No .close() needed - connection pool handles lifecycle


def get_redis_stats() -> Dict[str, Any]:
    """
    Get comprehensive Redis statistics.

    Returns:
        Dict with connection info, memory usage, and key counts
    """
    r = get_redis_client(decode_responses=False)
    stats = {
        "connected": False,
        "host": REDIS_HOST,
        "port": REDIS_PORT,
    }

    try:
        # Test connection
        r.ping()
        stats["connected"] = True

        # Get server info
        info = r.info()
        stats["redis_version"] = info.get("redis_version")
        stats["used_memory_human"] = info.get("used_memory_human")
        stats["used_memory_peak_human"] = info.get("used_memory_peak_human")
        stats["connected_clients"] = info.get("connected_clients")
        stats["total_keys"] = r.dbsize()

        # Count keys by type
        stats["key_counts"] = {
            "feast_online": 0,
            "streaming": 0,
            "binary": 0,
            "other": 0,
        }

        # Sample keys to estimate distribution
        cursor = 0
        sample_size = 0
        max_sample = 1000

        while sample_size < max_sample:
            cursor, keys = r.scan(cursor, count=100)
            for key in keys:
                try:
                    key_str = key.decode("utf-8")
                    if key_str.startswith(STREAMING_PREFIX):
                        stats["key_counts"]["streaming"] += 1
                    elif key_str.startswith(FEAST_ONLINE_PREFIX) or "feast" in key_str.lower():
                        stats["key_counts"]["feast_online"] += 1
                    else:
                        stats["key_counts"]["other"] += 1
                except UnicodeDecodeError:
                    stats["key_counts"]["binary"] += 1
                sample_size += 1

            if cursor == 0:
                break

        stats["sample_size"] = sample_size

    except Exception as e:
        stats["error"] = str(e)
    # No .close() needed - connection pool handles lifecycle

    return stats


def clear_streaming_features(customer_id: Optional[str] = None) -> int:
    """
    Clear streaming features from Redis.

    Args:
        customer_id: If provided, clear only this customer's features.
                    If None, clear ALL streaming features.

    Returns:
        Number of keys deleted
    """
    r = get_streaming_client()
    deleted = 0

    try:
        if customer_id:
            key = f"{STREAMING_PREFIX}{customer_id}"
            deleted = r.delete(key)
        else:
            # Clear all streaming keys
            cursor = 0
            keys_to_delete = []
            while True:
                cursor, keys = r.scan(cursor, match=f"{STREAMING_PREFIX}*", count=100)
                keys_to_delete.extend(keys)
                if cursor == 0:
                    break

            if keys_to_delete:
                deleted = r.delete(*keys_to_delete)

    except Exception as e:
        logger.error(f"Error clearing streaming features: {e}")
    # No .close() needed - connection pool handles lifecycle

    return deleted


def migrate_feast_keys_to_v2() -> Dict[str, Any]:
    """
    Check if there are binary (v3) Feast keys that need migration.

    Note: This does NOT perform migration - it only reports what would need
    to be migrated. Actual migration requires re-materializing features.

    Returns:
        Dict with migration assessment
    """
    r = get_redis_client(decode_responses=False)
    result = {
        "binary_keys_found": 0,
        "string_keys_found": 0,
        "needs_migration": False,
        "recommendation": "",
    }

    try:
        cursor = 0
        sample_count = 0
        max_sample = 500

        while sample_count < max_sample:
            cursor, keys = r.scan(cursor, count=100)
            for key in keys:
                try:
                    key.decode("utf-8")
                    result["string_keys_found"] += 1
                except UnicodeDecodeError:
                    result["binary_keys_found"] += 1
                sample_count += 1

            if cursor == 0:
                break

        if result["binary_keys_found"] > 0:
            result["needs_migration"] = True
            result["recommendation"] = (
                "Binary Feast keys detected (v3 format). To migrate to human-readable keys:\n"
                "1. Update feast_repo/feature_store.yaml: entity_key_serialization_version: 2\n"
                "2. Clear existing online store: feast teardown (or flush Redis)\n"
                "3. Re-materialize features: feast materialize\n"
                "Note: This will cause temporary feature unavailability."
            )
        else:
            result["recommendation"] = "All keys are human-readable. No migration needed."

    except Exception as e:
        result["error"] = str(e)
    # No .close() needed - connection pool handles lifecycle

    return result


# Export public functions
__all__ = [
    "get_redis_client",
    "get_streaming_client",
    "get_feast_client",
    "inspect_redis_keys",
    "get_streaming_features",
    "set_streaming_features",
    "get_redis_stats",
    "clear_streaming_features",
    "migrate_feast_keys_to_v2",
    "REDIS_HOST",
    "REDIS_PORT",
    "FEAST_ONLINE_PREFIX",
    "STREAMING_PREFIX",
]
