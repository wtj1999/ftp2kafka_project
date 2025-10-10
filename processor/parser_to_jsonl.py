from fetcher.fetcher import FTPFetcher
from processor.process_csv_parser import process_csv_to_json
from processor.result_csv_parser import result_csv_to_json
from processor.process_csv_parser_kelie import process_csv_to_json_kelie
from processor.result_csv_parser_kelie import result_csv_to_json_kelie
import logging
import os
from dotenv import load_dotenv
load_dotenv()
from processor.jsonl_to_kafka import send_jsonl_to_kafka_confluent

FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_ROOT = os.getenv("FTP_ROOT", "/")   # 从哪个目录开始递归查找
LOCAL_WORKDIR = os.getenv("LOCAL_WORKDIR", "./tmp_fetch")  # 本地临时目录
PROCESSED_DB = os.getenv("PROCESSED_DB", "./processed.json")  # 已处理记录文件

from typing import Dict, Any, Optional, List
from confluent_kafka import Producer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def process_and_send_pairs(
    fetcher,
    kafka_conf: Dict[str, Any],
    kafka_conf1: Dict[str, Any],
    topic_record: str,
    topic_step: Optional[str],
    delete_csv_after_send: bool = True,
    dry_run: bool = True,
    delete_jsonl_after_send: bool = False
) -> Dict[str, Any]:
    """
    fetcher: FTPFetcher 实例（实现 connect(), fetch_new_pairs()）
    processed_db: ProcessedDB 的实例（实现 is_processed / mark_processed）
    kafka_conf: confluent_kafka Producer 配置
    topic_record / topic_step: kafka topic
    """
    results = {"pairs_total": 0, "pairs_skipped": 0, "pairs_processed": 0, "kafka_sent": 0, "kafka_failed": 0}

    fetcher.connect()
    pairs = fetcher.fetch_new_pairs()
    results["pairs_total"] = len(pairs)
    logger.info("发现 %d 对文件需要处理", len(pairs))

    producer = Producer(kafka_conf)
    producer1 = Producer(kafka_conf1)

    for p in pairs:
        local_record = p.get("local_record")
        local_step = p.get("local_step")
        pair_key = p.get("pair_key")
        meta_str = p.get("meta_str")


        if pair_key is None:
            logger.error("无法确定 pair_key，跳过 p=%s", p)
            results["pairs_skipped"] += 1
            continue

        remote_meta = {"meta_str": meta_str}

        try:
            already = fetcher.processed_db.is_processed(pair_key, remote_meta)
        except Exception:
            logger.exception("processed_db.is_processed 出错，继续处理以免遗漏")
            already = False

        if already:
            logger.info("已处理过，跳过: %s (meta=%s)", pair_key, meta_str)
            results["pairs_skipped"] += 1
            continue

        # 必须存在本地 record 文件
        if not local_record or not os.path.exists(local_record):
            logger.error("本地 record 文件不存在，跳过: %s", local_record)
            results["pairs_skipped"] += 1
            continue

        # 必须存在本地 record 文件
        if not local_step or not os.path.exists(local_step):
            logger.error("本地 step 文件不存在，跳过: %s", local_step)
            results["pairs_skipped"] += 1
            continue

        logger.info("开始处理 pair_key=%s local_record=%s local_step=%s", pair_key, local_record, local_step)
        try:
            is_kelie = "科列" in local_record
            # 1) 生成 record jsonl
            rec_jsonl = local_record.replace(".csv", "_processed.jsonl")
            if os.path.exists(rec_jsonl):
                logger.info("record jsonl: %s 已存在, 跳过生成", rec_jsonl)
            else:
                logger.info("开始生成 record jsonl: %s", rec_jsonl)
                if is_kelie:
                    process_csv_to_json_kelie(csv_path=local_record, out_jsonl_path=rec_jsonl)
                else:
                    process_csv_to_json(csv_path=local_record, out_jsonl_path=rec_jsonl)

            # 2) 生成 step jsonl
            step_jsonl = local_step.replace(".csv", "_processed.jsonl")
            if os.path.exists(step_jsonl):
                logger.info("step jsonl: %s 已存在, 跳过生成", step_jsonl)
            else:
                logger.info("开始生成 step jsonl: %s", step_jsonl)
                if is_kelie:
                    result_csv_to_json_kelie(csv_path=local_step, out_jsonl_path=step_jsonl)
                else:
                    result_csv_to_json(csv_path=local_step, out_jsonl_path=step_jsonl)

            if dry_run:
                logger.info("[dry_run] 已生成 jsonl，但不发送：%s , %s", rec_jsonl, step_jsonl)
                fetcher.mark_processed(pair_key, remote_meta)
                results["pairs_processed"] += 1
                continue

            # 发送 record jsonl
            rec_exists = bool(rec_jsonl and os.path.exists(rec_jsonl))
            rec_res = {"sent": 0, "failed": 0}
            if rec_exists:
                try:
                    rec_res = send_jsonl_to_kafka_confluent(rec_jsonl, topic_record, producer)
                    results["kafka_sent"] += rec_res.get("sent", 0)
                    results["kafka_failed"] += rec_res.get("failed", 0)
                except Exception as e:
                    logger.exception("发送 record jsonl 到 Kafka 失败: %s. 错误: %s", rec_jsonl, e)
                    results["kafka_failed"] += 1
            else:
                logger.info("record jsonl 文件不存在或未生成，跳过发送: %s", rec_jsonl)

            # 发送 step jsonl
            step_exists = bool(step_jsonl and os.path.exists(step_jsonl))
            step_res = {"sent": 0, "failed": 0}
            if step_exists:
                try:
                    step_res = send_jsonl_to_kafka_confluent(step_jsonl, topic_step, producer1)
                    results["kafka_sent"] += step_res.get("sent", 0)
                    results["kafka_failed"] += step_res.get("failed", 0)
                except Exception as e:
                    logger.exception("发送 step jsonl 到 Kafka 失败: %s. 错误: %s", step_jsonl, e)
                    results["kafka_failed"] += 1
            else:
                logger.info("step jsonl 文件不存在或未生成，跳过发送: %s",step_jsonl)

            # 5) 若 Kafka 全部无失败 -> 标记 processed 并删除本地文件（若配置）
            if (rec_res.get("failed", 0) == 0) and (step_res.get("failed", 0) == 0):
                fetcher.mark_processed(pair_key, remote_meta)
                results["pairs_processed"] += 1

                if delete_jsonl_after_send:
                    for fp in (rec_jsonl, step_jsonl):
                        if fp and os.path.exists(fp):
                            try:
                                os.remove(fp)
                                logger.info("删除本地 jsonl: %s", fp)
                            except Exception:
                                logger.exception("删除 jsonl 失败: %s", fp)

                if delete_csv_after_send:
                    removed_dirs = set()
                    for fp in (local_record, local_step):
                        if fp and os.path.exists(fp):
                            try:
                                os.remove(fp)
                                logger.info("删除本地 csv: %s", fp)
                            except Exception:
                                logger.exception("删除本地 csv 失败: %s", fp)

                            else:
                                dirpath = os.path.dirname(fp)
                                if dirpath:
                                    removed_dirs.add(os.path.abspath(dirpath))

                    base_stop = None
                    try:
                        base_stop = os.path.abspath(fetcher.local_workdir) if getattr(fetcher, "local_workdir",
                                                                                      None) else None
                    except Exception:
                        base_stop = None

                    for dirpath in removed_dirs:
                        cur = dirpath
                        while True:
                            # 不越界删除：若到达基准目录则停止（基准目录不删除）
                            if base_stop and os.path.abspath(cur) == base_stop:
                                logger.debug("达到基准目录，不再向上删除: %s", cur)
                                break
                            try:
                                # 尝试删除目录（仅在空时生效）
                                os.rmdir(cur)
                                logger.info("删除空目录: %s", cur)
                            except OSError as e:
                                # 目录非空或无法删除 -> 停止向上删除
                                logger.debug("目录非空或无法删除，停止清理该分支: %s (%s)", cur, e)
                                break
                            except Exception as e:
                                logger.exception("删除目录出错: %s (%s)", cur, e)
                                break

                            # 向上一级目录继续尝试
                            parent = os.path.dirname(cur)
                            # 若已经到达根或父目录与当前相同（安全终止）
                            if not parent or parent == cur:
                                break
                            cur = parent
            else:
                logger.error("Kafka 发送出现失败，未标记 processed: %s", pair_key)

        except Exception:
            logger.exception("处理 pair_key 时出错: %s", pair_key)

    try:
        producer.flush(timeout=30.0)
    except Exception:
        logger.exception("producer.flush 出错")

    return results

