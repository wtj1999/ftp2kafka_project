# poller.py
import os
import time
import signal
import logging
import traceback
from threading import Event
from typing import Dict, Any, Optional


from fetcher.fetcher import FTPFetcher
from processor.parser_to_jsonl import process_and_send_pairs

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
MAX_BACKOFF = int(os.getenv("MAX_BACKOFF", "300"))
INITIAL_BACKOFF = int(os.getenv("INITIAL_BACKOFF", "5"))

FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_ROOT = os.getenv("FTP_ROOT", "/")
LOCAL_WORKDIR = os.getenv("LOCAL_WORKDIR", "./tmp_fetch")
PROCESSED_DB = os.getenv("PROCESSED_DB", "./processed.json")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_RECORD = os.getenv("TOPIC_RECORD", "topic_record")
TOPIC_STEP = os.getenv("TOPIC_STEP", "topic_step")
TOPIC_VEHICLE = os.getenv("TOPIC_VEHICLE", "topic_vehicle")

# logging
logger = logging.getLogger("ftp2kafka.poller")
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

_stop_event = Event()

def _signal_handler(signum, frame):
    logger.info("收到终止信号 %s，等待当前批次完成后退出...", signum)
    _stop_event.set()

# register signal handlers
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

def make_fetcher():
    fetcher = FTPFetcher(
        host=FTP_HOST,
        port=FTP_PORT,
        user=FTP_USER,
        password=FTP_PASS,
        root=FTP_ROOT,
        local_workdir=LOCAL_WORKDIR,
        processed_db_path=PROCESSED_DB,
    )
    return fetcher

def make_kafka_conf() -> Dict[str, Any]:
    return {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": 1,
    }

def main_loop():
    fetcher = make_fetcher()
    kafka_conf = make_kafka_conf()

    backoff = INITIAL_BACKOFF

    while not _stop_event.is_set():
        try:
            logger.info("开始一次轮询，调用 process_and_send_pairs ...")
            results = process_and_send_pairs(
                fetcher=fetcher,
                kafka_conf=kafka_conf,
                topic_record=TOPIC_RECORD,
                topic_step=TOPIC_STEP,
                topic_vehicle=TOPIC_VEHICLE,
                delete_csv_after_send=True,
                dry_run=False,
                delete_jsonl_after_send=True
            )
            logger.info("本次轮询结果: %s", results)
            backoff = INITIAL_BACKOFF

        except Exception as e:
            logger.exception("轮询/处理发生未捕获异常：%s\n%s", e, traceback.format_exc())
            time_to_sleep = min(backoff, MAX_BACKOFF)
            logger.warning("发生异常，休眠 %s 秒后重试（backoff）", time_to_sleep)
            for _ in range(time_to_sleep):
                if _stop_event.is_set():
                    break
                time.sleep(1)
            backoff = min(backoff * 2, MAX_BACKOFF)
            continue

        logger.info("等待 %s 秒后进行下一次轮询...", POLL_INTERVAL)
        slept = 0
        while slept < POLL_INTERVAL and not _stop_event.is_set():
            time.sleep(1)
            slept += 1

    logger.info("轮询被请求停止，退出 main_loop。")

if __name__ == "__main__":
    logger.info("启动 FTP -> Kafka poller ...")
    main_loop()
