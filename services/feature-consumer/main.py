import json
import time
import redis
import logging
from datetime import datetime, timezone
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError

from services.common.config import KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, CITY
from services.common.time_utils import floor_timestamp, window_ttl_seconds

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration (confluent-kafka compatible options)
KAFKA_CONFIG = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": f"{CITY}-feature-service",
    "auto.offset.reset": "latest",
    # Performance tuning for high volume
    "fetch.min.bytes": 1024,              # Wait for at least 1KB of data
    "fetch.wait.max.ms": 100,             # Max wait time for fetch.min.bytes
    "session.timeout.ms": 30000,          # 30 second session timeout
    "heartbeat.interval.ms": 10000,       # 10 second heartbeat
    "queued.min.messages": 10000,         # Min messages to keep in local queue
    "queued.max.messages.kbytes": 65536,  # 64MB local queue
}

# Topics
RIDE_TOPIC = f"rides.requested.{CITY}"
DRIVER_TOPIC = f"drivers.location.{CITY}"
TOPICS = [RIDE_TOPIC, DRIVER_TOPIC]

# Feature configuration
WINDOWS_MINUTES = [1, 5, 15]
AVG_IDLE_SPEED_KMH = 12

# Batching configuration
BATCH_SIZE = 100              # Number of events to batch before flushing to Redis
BATCH_TIMEOUT_MS = 200        # Max time to wait before flushing batch


def get_redis_client():
    """Retry Redis connection with connection pooling"""
    while True:
        try:
            pool = redis.ConnectionPool(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                max_connections=10,
                socket_connect_timeout=5,
                socket_keepalive=True,
            )
            client = redis.Redis(connection_pool=pool)
            client.ping()
            logger.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return client
        except Exception as e:
            logger.warning(f"Waiting for Redis... {e}")
            time.sleep(3)


def get_consumer():
    """Retry Kafka connection"""
    while True:
        try:
            consumer = Consumer(KAFKA_CONFIG)
            consumer.list_topics(timeout=5)
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return consumer
        except Exception as e:
            logger.warning(f"Waiting for Kafka... {e}")
            time.sleep(3)


def wait_for_topics(consumer: Consumer, topics: list[str], timeout: int = 120):
    """Wait for all topics to become available in Kafka"""
    start_time = time.time()
    pending_topics = set(topics)
    
    while pending_topics and (time.time() - start_time < timeout):
        try:
            metadata = consumer.list_topics(timeout=10)
            
            for topic in list(pending_topics):
                if topic in metadata.topics:
                    topic_metadata = metadata.topics[topic]
                    if topic_metadata.error is None:
                        logger.info(f"Topic {topic} is available with {len(topic_metadata.partitions)} partitions")
                        pending_topics.remove(topic)
                    else:
                        logger.warning(f"Topic {topic} has error: {topic_metadata.error}")
                        
        except Exception as e:
            logger.warning(f"Error checking topic metadata: {e}")
        
        if pending_topics:
            logger.info(f"Waiting for topics: {pending_topics}")
            time.sleep(3)
    
    if pending_topics:
        raise TimeoutError(f"Topics {pending_topics} not available after {timeout} seconds")
    
    logger.info("All topics are available")


class FeatureAggregator:
    """
    Batches feature updates and flushes to Redis efficiently.
    
    Instead of writing each event individually, this aggregates updates
    in memory and flushes them in batches, reducing Redis round-trips.
    """
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.pending_updates: dict[str, dict] = defaultdict(lambda: {
            "ride_requests": 0,
            "available_drivers": 0,
            "active_drivers": 0,
            "deadhead_km_sum": 0.0,
            "idle_events": 0,
            "metadata": None,
        })
        self.last_flush = time.time()
        self.events_since_flush = 0
    
    def add_ride_event(self, event: dict) -> bool:
        """Add a ride event to the batch"""
        try:
            h3_index = event.get("h3_res8")
            timestamp_str = event.get("timestamp")
            
            if not h3_index or not timestamp_str:
                return False
            
            ts = datetime.fromisoformat(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            
            for window in WINDOWS_MINUTES:
                window_start = floor_timestamp(ts, window)
                window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
                
                self.pending_updates[window_key]["ride_requests"] += 1
                
                if self.pending_updates[window_key]["metadata"] is None:
                    self.pending_updates[window_key]["metadata"] = {
                        "window_minutes": window,
                        "h3_res8": h3_index,
                        "window_start": window_start.isoformat(),
                        "ttl": window_ttl_seconds(window),
                    }
            
            self.events_since_flush += 1
            return True
            
        except Exception as e:
            logger.error(f"Error adding ride event: {e}")
            return False
    
    def add_driver_event(self, event: dict) -> bool:
        """Add a driver event to the batch"""
        try:
            h3_index = event.get("h3_res8")
            timestamp_str = event.get("timestamp")
            status = event.get("status")
            idle_seconds = event.get("idle_seconds", 0)
            
            if not h3_index or not timestamp_str:
                return False
            
            ts = datetime.fromisoformat(timestamp_str)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            
            deadhead_km = (
                idle_seconds * AVG_IDLE_SPEED_KMH / 3600
                if status == "available"
                else 0
            )
            
            for window in WINDOWS_MINUTES:
                window_start = floor_timestamp(ts, window)
                window_key = f"{CITY}:{h3_index}:{window}m:{window_start.isoformat()}"
                
                if status == "available":
                    self.pending_updates[window_key]["available_drivers"] += 1
                    self.pending_updates[window_key]["deadhead_km_sum"] += deadhead_km
                    self.pending_updates[window_key]["idle_events"] += 1
                else:
                    self.pending_updates[window_key]["active_drivers"] += 1
                
                if self.pending_updates[window_key]["metadata"] is None:
                    self.pending_updates[window_key]["metadata"] = {
                        "window_minutes": window,
                        "h3_res8": h3_index,
                        "window_start": window_start.isoformat(),
                        "ttl": window_ttl_seconds(window),
                    }
            
            self.events_since_flush += 1
            return True
            
        except Exception as e:
            logger.error(f"Error adding driver event: {e}")
            return False
    
    def should_flush(self) -> bool:
        """Check if we should flush the batch"""
        if self.events_since_flush >= BATCH_SIZE:
            return True
        if (time.time() - self.last_flush) * 1000 >= BATCH_TIMEOUT_MS:
            return True
        return False
    
    def flush(self) -> int:
        """Flush all pending updates to Redis"""
        if not self.pending_updates:
            return 0
        
        try:
            pipe = self.redis.pipeline()
            keys_updated = 0
            
            for window_key, updates in self.pending_updates.items():
                metadata = updates["metadata"]
                if metadata is None:
                    continue
                
                # Batch all increments for this key
                if updates["ride_requests"] > 0:
                    pipe.hincrby(window_key, "ride_requests", updates["ride_requests"])
                
                if updates["available_drivers"] > 0:
                    pipe.hincrby(window_key, "available_drivers", updates["available_drivers"])
                
                if updates["active_drivers"] > 0:
                    pipe.hincrby(window_key, "active_drivers", updates["active_drivers"])
                
                if updates["deadhead_km_sum"] > 0:
                    pipe.hincrbyfloat(window_key, "deadhead_km_sum", updates["deadhead_km_sum"])
                
                if updates["idle_events"] > 0:
                    pipe.hincrby(window_key, "idle_events", updates["idle_events"])
                
                # Metadata (idempotent)
                pipe.hsetnx(window_key, "window_minutes", metadata["window_minutes"])
                pipe.hsetnx(window_key, "h3_res8", metadata["h3_res8"])
                pipe.hsetnx(window_key, "window_start", metadata["window_start"])
                
                # TTL
                pipe.expire(window_key, metadata["ttl"])
                
                keys_updated += 1
            
            pipe.execute()
            
            # Reset state
            flushed_events = self.events_since_flush
            self.pending_updates.clear()
            self.events_since_flush = 0
            self.last_flush = time.time()
            
            logger.debug(f"Flushed {flushed_events} events to {keys_updated} keys")
            return flushed_events
            
        except redis.RedisError as e:
            logger.error(f"Redis error during flush: {e}", exc_info=True)
            return 0


def main():
    """Main consumer loop with batched processing"""
    redis_client = get_redis_client()
    consumer = get_consumer()
    
    # Wait for all topics to be created by producer
    wait_for_topics(consumer, TOPICS)
    
    consumer.subscribe(TOPICS)
    
    logger.info("=" * 60)
    logger.info("Feature consumer started (batched processing enabled)")
    logger.info("=" * 60)
    logger.info(f"  Subscribed to: {TOPICS}")
    logger.info(f"  Window sizes: {WINDOWS_MINUTES} minutes")
    logger.info(f"  Batch size: {BATCH_SIZE} events")
    logger.info(f"  Batch timeout: {BATCH_TIMEOUT_MS}ms")
    logger.info("=" * 60)
    
    aggregator = FeatureAggregator(redis_client)
    
    ride_count = 0
    driver_count = 0
    flush_count = 0
    start_time = time.time()
    last_log_time = start_time
    
    try:
        while True:
            msg = consumer.poll(0.1)  # Shorter poll for better batching
            
            if msg is None:
                # No message, but check if we should flush
                if aggregator.should_flush():
                    aggregator.flush()
                    flush_count += 1
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {msg.partition()}")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.warning(f"Topic temporarily unavailable, waiting...")
                    time.sleep(5)
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue
            
            try:
                event = json.loads(msg.value().decode("utf-8"))
                topic = msg.topic()
                
                # Add to batch based on topic
                if topic == RIDE_TOPIC:
                    if aggregator.add_ride_event(event):
                        ride_count += 1
                elif topic == DRIVER_TOPIC:
                    if aggregator.add_driver_event(event):
                        driver_count += 1
                
                # Check if we should flush
                if aggregator.should_flush():
                    aggregator.flush()
                    flush_count += 1
                
                # Log summary every 10 seconds
                if time.time() - last_log_time >= 10:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"[{elapsed:.0f}s] "
                        f"Rides: {ride_count} ({ride_count/elapsed:.1f}/s) | "
                        f"Drivers: {driver_count} ({driver_count/elapsed:.1f}/s) | "
                        f"Flushes: {flush_count}"
                    )
                    last_log_time = time.time()
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
    
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        # Final flush
        aggregator.flush()
        
        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Final statistics:")
        logger.info(f"  Runtime: {elapsed:.1f}s")
        logger.info(f"  Total ride events: {ride_count} ({ride_count/elapsed:.1f}/s)")
        logger.info(f"  Total driver events: {driver_count} ({driver_count/elapsed:.1f}/s)")
        logger.info(f"  Total flushes: {flush_count}")
        logger.info("=" * 60)
        
        logger.info("Closing consumer...")
        consumer.close()
        redis_client.close()
        logger.info("Feature consumer stopped")


if __name__ == "__main__":
    main()