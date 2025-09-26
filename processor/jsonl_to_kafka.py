# send_with_confluent.py
import os
import json
import time
import logging
from typing import Iterable, Optional, Dict, Any
from confluent_kafka import Producer, KafkaError

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


def send_jsonl_to_kafka_confluent(
    jsonl_path: str,
    topic: str,
    producer: Optional[Producer] = None,
    sync: bool = True,
    retries: int = 3,
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
            # 这里无法直接从 Producer 获取未决消息数，通常用 flush 来阻塞直到队列为空或超过 timeout
            # 但我们想要让 delivery_cb 统计 sent/failed，所以用 flush:
            try:
                producer.flush(timeout=0)
            except TypeError:
                # 某些版本 .flush() 不接受 timeout, 忽略
                pass

            # 退出条件：当总处理数 (sent+failed) >= 文件行数（粗略），但我们没有行数计数器——使用 flush 最稳
            # 直接调用 flush(blocking)：
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
