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
    """
    conf 示例:
    {
        "bootstrap.servers": "kafka1:9092,kafka2:9092",
        "acks": "all",                # 确保所有副本确认
        "enable.idempotence": True,   # 幂等（需要 broker 支持）
        "compression.type": "lz4",
        "retries": 5,
        "linger.ms": 5,
        # 其它 librdkafka 配置项...
    }
    """
    return Producer(conf)

def make_admin_client(conf: dict) -> AdminClient:
    # AdminClient 使用与 Producer 相同的 bootstrap/认证配置
    return AdminClient(conf)

def ensure_topic_exists(admin_conf: dict, topic: str,
                        num_partitions: int = 1,
                        replication_factor: Optional[int] = None,
                        topic_config: Optional[dict] = None,
                        create_timeout: float = 10.0) -> bool:
    """
    确保 topic 存在；不存在则创建。返回 True 表示 topic 最终存在（已创建或已存在），False 表示失败。
    admin_conf: 与 Producer 类似的配置 dict（至少包含 bootstrap.servers 和可能的认证）
    replication_factor: 若为 None，函数会尝试根据 broker 数量选择合理的值（min(3, broker_count)）
    topic_config: dict，额外的 topic-level configs（例如 {"retention.ms": "604800000"}）
    """
    admin = make_admin_client(admin_conf)
    try:
        md = admin.list_topics(timeout=10)
        # 若已经存在直接返回 True
        if topic in md.topics and md.topics[topic].error is None:
            logger.info("Topic %s already exists.", topic)
            return True
        # 计算合适的 replication_factor（如果未指定）
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
        # fs 是 dict topic->future
        fut = fs.get(topic)
        try:
            fut.result(create_timeout)  # 若失败会抛异常
            logger.info("Topic %s created (partitions=%d, rf=%d).", topic, num_partitions, replication_factor)
            return True
        except KafkaException as e:
            # 处理已存在或其他错误
            err = e.args[0]
            # TOPIC_ALREADY_EXISTS 的情况下也算成功
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

'''
def send_jsonl_to_kafka_confluent(
    jsonl_path: str,
    topic: str,
    producer: Optional[Producer] = None,
    sync: bool = True,
    retries: int = 5,
    flush_timeout: float = 30.0
) -> Dict[str, int]:
    """
    使用 confluent_kafka.Producer 逐行发送 jsonl。
    - 若传入 producer 则复用，否则根据 producer_conf 创建并在函数结束时 flush/close（Producer 无 explicit close）。
    - sync=True 时会等待交付回调（通过 poll + delivery flag）；sync=False 则异步发送并在最后 flush。
    返回 {"sent": n_sent, "failed": n_failed}
    """
    close_prod = False
    if producer is None:
        producer = make_confluent_producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        close_prod = True

    sent = 0
    failed = 0

    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(jsonl_path)

    # 用于等待每条消息送达的机制（sync 模式会等）
    deliveries = {}

    def delivery_cb(err, msg, lineno=None):
        nonlocal sent, failed
        if err is not None:
            logger.warning("Delivery failed for line %s: %s", lineno, err)
            failed += 1
        else:
            sent += 1

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
            while attempt <= retries:
                attempt += 1
                try:
                    # note: value must be bytes
                    payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
                    # 使用 lambda 闭包把 lineno 传到回调
                    producer.produce(topic, value=payload,
                                     callback=(lambda err, msg, ln=lineno: delivery_cb(err, msg, ln)))
                    # poll 以触发回调（非阻塞），当 sync=True 我们也会 poll 等待全部回调
                    producer.poll(0)  # 触发轮询处理队列中的事件
                    break
                except BufferError as e:
                    # 本地队列满（非常大吞吐时可能发生），等待并重试
                    last_exc = e
                    logger.warning("Local producer queue full, wait and retry (attempt %d): %s", attempt, e)
                    time.sleep(min(attempt, 5.0))
                except KafkaError as e:
                    last_exc = e
                    logger.warning("KafkaError on produce (attempt %d): %s", attempt, e)
                    time.sleep(min(attempt, 5.0))
                except Exception as e:
                    last_exc = e
                    logger.exception("Unexpected produce error (attempt %d): %s", attempt, e)
                    time.sleep(1)

            else:
                # 所有重试失败
                logger.error("Giving up sending line %d from %s after %d attempts: %s",
                             lineno, jsonl_path, retries, last_exc)
                failed += 1
                continue

    # 如果需要同步确认每条消息（sync=True），循环 poll 直到本地已交付所有或超时
    if sync:
        start = time.time()
        while True:
            # poll 等待交付回调触发
            producer.poll(0.1)
            try:
                producer.flush(timeout=0)
            except TypeError:
                pass
            break

    # 最后进行一次 flush（等待 outstanding）
    try:
        producer.flush(timeout=flush_timeout)
    except Exception as e:
        logger.warning("producer.flush() exception: %s", e)

    # 如果是我们创建的 producer，不需要显式关闭（librdkafka 内部释放）
    # 但可以进行一次 poll(0) 触发事件
    if close_prod:
        producer.poll(0)

    return {"sent": sent, "failed": failed}
'''

def send_jsonl_to_kafka_confluent(
    jsonl_path: str,
    topic: str,
    producer: Optional[Producer] = None,
    admin_conf: Optional[dict] = None,
    ensure_topic: bool = True,
    sync: bool = True,
    produce_retries: int = 5,          # produce() 层面的重试（本地队列/临时错误）
    delivery_retry_rounds: int = 3,    # 在 delivery_cb 报失败后对失败消息的重试轮数
    delivery_retry_backoff: float = 1.0,  # 首次重试等待秒数（指数退避）
    flush_timeout: float = 150.0
) -> Dict[str, int]:
    """
    逐行发送 jsonl，并在 delivery_cb 报失败后尝试重试失败的消息。
    返回 {"sent": n_sent, "failed": n_failed}
    """
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

    # 最终返回 sent/failed（注意 failed 包含 produce 失败与 delivery 失败）
    return {"sent": sent, "failed": failed}


