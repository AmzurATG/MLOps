"""
MLOps Streaming Module
======================

Kafka consumers for real-time fraud feature streaming.

Components:
- FraudStreamingConsumer: Consumes fraud transactions and updates Redis

Usage:
    from mlops.streaming import FraudStreamingConsumer

    consumer = FraudStreamingConsumer()
    consumer.run()
"""

from mlops.streaming.consumer import FraudStreamingConsumer

__all__ = ["FraudStreamingConsumer"]
