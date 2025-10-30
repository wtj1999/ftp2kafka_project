# send_with_confluent.py
import os
import json
import time
import logging
import threading
from typing import Iterable, Optional, Dict, Any
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic, KafkaException

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


def make_confluent_producer(conf: Dict[str, Any]) -> Producer:
    return Producer(conf)

def make_admin_client(conf: dict) -> AdminClient:
    return AdminClient(conf)

def ensure_topic_exists(admin_conf: dict, topic: str,
                        num_partitions: int = 1,
                        replication_factor: Optional[int] = None,
                        topic_config: Optional[dict] = None,
                        create_timeout: float = 10.0) -> bool:
    admin = make_admin_client(admin_conf)
    try:
        md = admin.list_topics(timeout=10)
        if topic in md.topics and md.topics[topic].error is None:
            logger.info("Topic %s already exists.", topic)
            return True
        broker_count = len(md.brokers)
        if broker_count <= 0:
            logger.warning("Can't determine broker count, default replication_factor to 1")
            broker_count = 1
        if replication_factor is None:
            replication_factor = min(3, broker_count) if broker_count >= 1 else 1

        new_topic = NewTopic(topic, num_partitions=num_partitions,
                             replication_factor=replication_factor,
                             config=topic_config or {})
        fs = admin.create_topics([new_topic], request_timeout=create_timeout)
        fut = fs.get(topic)
        try:
            fut.result(create_timeout)
            logger.info("Topic %s created (partitions=%d, rf=%d).", topic, num_partitions, replication_factor)
            return True
        except KafkaException as e:
            err = e.args[0]
            if hasattr(err, 'code') and err.code().name == 'TOPIC_ALREADY_EXISTS':
                logger.info("Topic %s already exists (race).", topic)
                return True
            logger.error("Failed to create topic %s: %s", topic, e)
            return False
        except Exception as e:
            logger.exception("Unexpected error creating topic %s: %s", topic, e)
            return False
    except Exception as e:
        logger.exception("Failed to fetch metadata/create topic %s: %s", topic, e)
        return False

def send_jsonl_to_kafka_confluent(
    jsonl_path: str,
    topic: str,
    producer: Optional[Producer] = None,
    admin_conf: Optional[dict] = None,
    ensure_topic: bool = True,
    sync: bool = True,
    produce_retries: int = 5,
    delivery_retry_rounds: int = 3,
    delivery_retry_backoff: float = 1.0,
    flush_timeout: float = 150.0
) -> Dict[str, int]:
    close_prod = False
    if producer is None:
        producer = make_confluent_producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        close_prod = True

    if ensure_topic:
        admin_conf = admin_conf or {"bootstrap.servers": KAFKA_BOOTSTRAP}
        ok = ensure_topic_exists(admin_conf, topic, num_partitions=1, replication_factor=None, topic_config=None)
        if not ok:
            logger.error("Topic %s does not exist and could not be created. Aborting send.", topic)
            if close_prod:
                producer.poll(0)
            return {"sent": 0, "failed": 0}

    sent = 0
    failed = 0
    total_attempted = 0
    delivered_count = 0

    failed_msgs = []
    lock = threading.Lock()

    def delivery_cb(err, msg, lineno=None, orig_obj=None):
        nonlocal sent, failed, delivered_count
        if err is not None:
            logger.warning("Delivery failed for line %s: %s", lineno, err)
            with lock:
                failed_msgs.append((lineno, orig_obj))
            failed += 1
        else:
            sent += 1
        delivered_count += 1

    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(jsonl_path)

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if raw == "":
                continue
            try:
                obj = json.loads(raw)
            except Exception as e:
                logger.exception("Invalid JSON in %s line %d: %s", jsonl_path, lineno, e)
                failed += 1
                continue

            attempt = 0
            last_exc = None
            while attempt <= produce_retries:
                attempt += 1
                try:
                    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                    producer.produce(topic, value=payload,
                                     callback=(lambda err, msg, ln=lineno, orig=obj: delivery_cb(err, msg, ln, orig)))
                    producer.poll(0)
                    total_attempted += 1
                    break
                except BufferError as e:
                    last_exc = e
                    logger.warning("Local producer queue full, retrying produce (attempt %d): %s", attempt, e)
                    time.sleep(min(0.5 * attempt, 5.0))
                except KafkaError as e:
                    last_exc = e
                    logger.warning("KafkaError on produce (attempt %d): %s", attempt, e)
                    time.sleep(min(0.5 * attempt, 5.0))
                except Exception as e:
                    last_exc = e
                    logger.exception("Unexpected produce error (attempt %d): %s", attempt, e)
                    time.sleep(1)
            else:
                logger.error("Give up produce for line %d after %d attempts: %s", lineno, produce_retries, last_exc)
                failed += 1
                continue

    if sync and total_attempted > 0:
        start = time.time()
        while True:
            producer.poll(0.1)
            if delivered_count >= total_attempted:
                break
            if time.time() - start > flush_timeout:
                logger.warning("Wait delivery callbacks timeout: %d/%d", delivered_count, total_attempted)
                break

    try:
        producer.flush(timeout=flush_timeout)
    except Exception as e:
        logger.warning("producer.flush() exception: %s", e)

    with lock:
        current_failed = list(failed_msgs)
        failed_msgs.clear()

    if current_failed:
        logger.warning("发现 %d 条交付失败消息，开始重试 %d 轮", len(current_failed), delivery_retry_rounds)
        for r in range(delivery_retry_rounds):
            if not current_failed:
                break
            backoff = delivery_retry_backoff * (2 ** r)
            logger.info("重试轮 %d, 等待 %.2fs, 重试 %d 条消息", r + 1, backoff, len(current_failed))
            time.sleep(backoff)
            retry_next = []
            for lineno, orig_obj in current_failed:
                try:
                    payload = json.dumps(orig_obj, ensure_ascii=False).encode("utf-8")
                    producer.produce(topic, value=payload,
                                     callback=(lambda err, msg, ln=lineno, orig=orig_obj: delivery_cb(err, msg, ln, orig)))
                    producer.poll(0)
                except Exception as e:
                    logger.warning("重试 produce 行 %s 失败: %s", lineno, e)
                    retry_next.append((lineno, orig_obj))
            start = time.time()
            while True:
                producer.poll(0.1)
                if time.time() - start > min(10.0, flush_timeout):
                    break
            with lock:
                next_failed = list(failed_msgs)
                failed_msgs.clear()
            merged = []
            merged.extend(next_failed)
            merged.extend(retry_next)
            seen_ln = set()
            deduped = []
            for it in merged:
                if it[0] not in seen_ln:
                    deduped.append(it)
                    seen_ln.add(it[0])
            current_failed = deduped

        if current_failed:
            failed_file = jsonl_path + ".failed"
            logger.error("重试后仍有 %d 条失败，写入 %s", len(current_failed), failed_file)
            try:
                with open(failed_file, "a", encoding="utf-8") as ff:
                    for lineno, obj in current_failed:
                        ff.write(json.dumps({"lineno": lineno, "obj": obj}, ensure_ascii=False) + "\n")
            except Exception:
                logger.exception("写入 failed 文件失败")
    if close_prod:
        producer.poll(0)
    return {"sent": sent, "failed": failed}

def send_jsonl_to_kafka_confluent1(
    jsonl_data,
    topic: str,
    producer: Optional[Producer] = None,
    admin_conf: Optional[dict] = None,
    ensure_topic: bool = True,
    produce_retries: int = 5,
):

    close_prod = False
    if producer is None:
        producer = make_confluent_producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        close_prod = True

    if ensure_topic:
        admin_conf = admin_conf or {"bootstrap.servers": KAFKA_BOOTSTRAP}
        ok = ensure_topic_exists(admin_conf, topic, num_partitions=1, replication_factor=None, topic_config=None)
        if not ok:
            logger.error("Topic %s does not exist and could not be created. Aborting send.", topic)
            if close_prod:
                producer.poll(0)
            return {"sent": 0, "failed": 0}

    sent = 0
    failed = 0
    total_attempted = 0
    delivered_count = 0

    failed_msgs = []
    lock = threading.Lock()

    def delivery_cb(err, msg, lineno=None, orig_obj=None):
        nonlocal sent, failed, delivered_count
        if err is not None:
            logger.warning("Delivery failed for line %s: %s", lineno, err)
            with lock:
                failed_msgs.append((lineno, orig_obj))
            failed += 1
        else:
            sent += 1
        delivered_count += 1

    attempt = 0
    last_exc = None
    while attempt <= produce_retries:
        attempt += 1
        try:
            payload = json.dumps(jsonl_data, ensure_ascii=False).encode("utf-8")
            producer.produce(topic, value=payload,
                             callback=(lambda err, msg, ln=1, orig=jsonl_data: delivery_cb(err, msg, ln, orig)))
            producer.poll(0)
            total_attempted += 1
            break
        except BufferError as e:
            last_exc = e
            logger.warning("Local producer queue full, retrying produce (attempt %d): %s", attempt, e)
            time.sleep(min(0.5 * attempt, 5.0))
        except KafkaError as e:
            last_exc = e
            logger.warning("KafkaError on produce (attempt %d): %s", attempt, e)
            time.sleep(min(0.5 * attempt, 5.0))
        except Exception as e:
            last_exc = e
            logger.exception("Unexpected produce error (attempt %d): %s", attempt, e)
            time.sleep(1)
    else:
        logger.error("Give up produce for vehicle to pack after %d attempts: %s",  produce_retries, last_exc)
        failed += 1


