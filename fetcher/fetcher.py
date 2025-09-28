# fetcher.py
import os
import json
import ftplib
import logging
import tempfile
from typing import Dict, List, Optional, Tuple
from pathlib import PurePosixPath
import time
import datetime
import posixpath
import errno
import socket
import functools
import traceback
import os.path as osp
from dotenv import load_dotenv
from fetcher.ftp_info import get_remote_file_info
from fetcher.ftp_file import list_files_for_date

load_dotenv()  # 如果使用 .env 文件

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("fetcher")

# ========== 配置（可以从环境变量读取，也可以在实例化时覆盖） ==========
FTP_HOST = os.getenv("FTP_HOST")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS", "")
FTP_ROOT = os.getenv("FTP_ROOT", "/")   # 从哪个目录开始递归查找
LOCAL_WORKDIR = os.getenv("LOCAL_WORKDIR", "./tmp_fetch")  # 本地临时目录
PROCESSED_DB = os.getenv("PROCESSED_DB", "./processed.json")  # 已处理记录文件
BRANCH_MAX = int(os.getenv("BRANCH_MAX", "14"))


# ========== 工具函数 ==========
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def safe_makedirs_for_file(path: str):
    ensure_dir(os.path.dirname(path))


def _safe_sendcmd(ftp: ftplib.FTP, cmd: str) -> Optional[str]:
    try:
        return ftp.sendcmd(cmd)
    except Exception:
        return None






# ========== 已处理记录管理 ==========
class ProcessedDB:
    def __init__(self, path: str = PROCESSED_DB):
        self.path = path
        self._data = {}  # prefix -> metadata dict
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception:
                logger.warning("无法读取 processed db，重新初始化：%s", self.path)
                self._data = {}

    def is_processed(self, pair_key: str, remote_meta: Dict) -> bool:
        """
        判断给定 pair_key 是否被处理过（通过比较记录的 meta 是否一致）
        这里 pair_key 是我们对一对文件生成的唯一 key（例如 remote_prefix）
        remote_meta 可包含 mdtm/size 拼接字符串
        我们只要 detect 如果已经以相同 meta 标记为 processed 则返回 True
        """
        rec = self._data.get(pair_key)
        if not rec:
            return False
        # compare stored meta (we store as string)
        stored = rec.get("meta")
        return stored == remote_meta.get("meta_str")

    def mark_processed(self, pair_key: str, remote_meta: Dict):
        self._data[pair_key] = {
            "meta": remote_meta.get("meta_str"),
            "time": time.time()
        }
        self._flush()

    def _flush(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.path)


# ========== 主 Fetcher 类 ==========
class FTPFetcher:
    def __init__(
        self,
        host: str = FTP_HOST,
        port: int = FTP_PORT,
        user: str = FTP_USER,
        password: str = FTP_PASS,
        root: str = FTP_ROOT,
        local_workdir: str = LOCAL_WORKDIR,
        processed_db_path: str = PROCESSED_DB,
        timeout: int = 30,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.root = root or "."
        self.local_workdir = local_workdir
        ensure_dir(self.local_workdir)
        self._conn: Optional[ftplib.FTP] = None
        self.timeout = timeout
        self.processed_db = ProcessedDB(processed_db_path)

    def connect(self):
        logger.info("连接 FTP: %s:%s", self.host, self.port)
        ftp = ftplib.FTP()
        ftp.connect(self.host, self.port, timeout=self.timeout)
        ftp.login(self.user, self.password)
        ftp.set_pasv(True)
        self._conn = ftp
        logger.info("FTP 登录成功: %s", self.user)

    def close(self):
        if self._conn:
            try:
                self._conn.quit()
            except Exception:
                try:
                    self._conn.close()
                except:
                    pass
            self._conn = None

    def _remote_full_path(self, p: str) -> str:
        # ensure posix style
        return p.replace("\\", "/")

    def find_pairs(self) -> Dict[str, Dict]:
        """
        在 FTP 上递归查找所有文件，识别成对的记录层/工步层：
        - 以后缀 '@@工步层.csv' / '@@记录层.csv' 匹配
        - 以文件名前缀（去掉后缀部分）作为 pair_key
        返回 mapping:
        {
          pair_key1: { 'step': remote_path, 'record': remote_path, 'step_info': {...}, 'record_info': {...} },
          ...
        }
        """
        if not self._conn:
            self.connect()
        ftp = self._conn
        today = datetime.date.today()  # 或者传入特定日期 datetime.date(2025, 9, 25)
        all_files = list_files_for_date(ftp, base_root=self.root, day=today, branch_max=BRANCH_MAX)

        # logger.info("开始递归列出 FTP 下 %s 所有文件 ...", self.root)
        # all_files = ftp_list_recursive(ftp, self.root)
        # logger.info("共找到 %d 个远程路径（含文件/可能的目录）", len(all_files))

        pairs = {}
        for remote in all_files:
            fname = posixpath.basename(remote)
            if fname.endswith("@@工步层.csv"):
                prefix = remote[:-len("@@工步层.csv")]
                rec = pairs.setdefault(prefix, {})
                rec["step"] = remote
            elif fname.endswith("@@记录层.csv"):
                prefix = remote[:-len("@@记录层.csv")]
                rec = pairs.setdefault(prefix, {})
                rec["record"] = remote
        # filter only those which have both
        valid_pairs = {}
        for prefix, info in pairs.items():
            if "step" in info and "record" in info:
                # fetch meta for both files
                step_info = {}#get_remote_file_info(ftp, info["step"])
                record_info = {}#get_remote_file_info(ftp, info["record"])
                # build meta string to detect changes: combine paths + mdtm + size
                meta_str = "|".join([
                    info["step"],
                    str(step_info.get("mdtm") or ""),
                    str(step_info.get("size") or ""),
                    info["record"],
                    str(record_info.get("mdtm") or ""),
                    str(record_info.get("size") or "")
                ])
                valid_pairs[prefix] = {
                    "step": info["step"],
                    "record": info["record"],
                    "step_info": step_info,
                    "record_info": record_info,
                    "meta_str": meta_str
                }
        logger.info("共匹配到 %d 个 step+record 成对文件", len(valid_pairs))
        return valid_pairs

    def _download_file(self, remote_path: str, local_path: str) -> bool:
        safe_makedirs_for_file(local_path)
        logger.info("下载远程文件: %s -> %s", remote_path, local_path)
        ftp = self._conn
        try:
            with open(local_path, "wb") as f:
                ftp.retrbinary("RETR " + remote_path, f.write)
            return True
        except Exception as e:
            logger.error("下载失败: %s ; %s", remote_path, e)
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except:
                pass
            return False

    def fetch_new_pairs(self) -> List[Dict]:
        """
        查找 FTP 上的新配对，下载到本地并返回本地文件信息列表。
        返回每个元素为 dict:
        {
          "pair_key": prefix,
          "remote_step": remote_path,
          "remote_record": remote_path,
          "local_step": local_path,
          "local_record": local_path,
          "meta_str": meta_str
        }
        """
        if not self._conn:
            self.connect()

        pairs = self.find_pairs()
        to_download = []
        for prefix, info in pairs.items():
            meta = {"meta_str": info["meta_str"]}
            if self.processed_db.is_processed(prefix, meta):
                logger.debug("已处理，跳过: %s", prefix)
                continue
            # add to download list
            to_download.append((prefix, info))

        results = []
        for prefix, info in to_download:
            # download both files into local_workdir/prefix/
            safe_local_dir = os.path.join(self.local_workdir, self._safe_filename(prefix).lstrip("_"))
            ensure_dir(safe_local_dir)
            local_step = os.path.join(safe_local_dir, posixpath.basename(info["step"]))
            local_record = os.path.join(safe_local_dir, posixpath.basename(info["record"]))

            if os.path.exists(local_step):
                logger.info("step 文件已存在，跳过下载: %s", local_step)
            else:
                ok1 = self._download_file(info["step"], local_step)
                if not ok1:
                    logger.error("step 下载失败，跳过该配对: %s", prefix)
                    continue

            if os.path.exists(local_record):
                logger.info("record 文件已存在，跳过下载: %s", local_record)
            else:
                ok2 = self._download_file(info["record"], local_record)
                if not ok2:
                    logger.error("record 下载失败，删除 step 并跳过配对: %s", prefix)
                    try:
                        os.remove(local_step)
                    except:
                        pass
                    continue

            results.append({
                "pair_key": prefix,
                "remote_step": info["step"],
                "remote_record": info["record"],
                "local_step": local_step,
                "local_record": local_record,
                "meta_str": info["meta_str"]
            })
            logger.info("已下载配对: %s -> %s", prefix, safe_local_dir)

        return results

    def mark_processed(self, pair_key: str, meta_str: Dict):
        self.processed_db.mark_processed(pair_key, meta_str)
        logger.info("标记为已处理: %s", pair_key)

    def cleanup_local(self, local_paths: List[str], remove_empty_parent: bool = True):
        """
        删除本地临时文件（通常在成功上传 Kafka 后调用）
        """
        for p in local_paths:
            try:
                if os.path.isdir(p):
                    logger.info("删除本地目录: %s", p)
                    # remove files under dir
                    for root, dirs, files in os.walk(p, topdown=False):
                        for fname in files:
                            try:
                                os.remove(os.path.join(root, fname))
                            except:
                                pass
                        for d in dirs:
                            try:
                                os.rmdir(os.path.join(root, d))
                            except:
                                pass
                    try:
                        os.rmdir(p)
                    except:
                        pass
                elif os.path.exists(p):
                    logger.info("删除本地文件: %s", p)
                    os.remove(p)
            except Exception:
                logger.exception("删除本地失败: %s", p)

        if remove_empty_parent:
            # try cleanup parent workdir empty dirs
            try:
                for d in os.listdir(self.local_workdir):
                    dd = os.path.join(self.local_workdir, d)
                    if os.path.isdir(dd) and not os.listdir(dd):
                        try:
                            os.rmdir(dd)
                        except:
                            pass
            except Exception:
                pass

    @staticmethod
    def _safe_filename(s: str) -> str:
        # convert remote prefix to safe local dir name
        if not s:
            return "_"
        safe = "".join([c if c.isalnum() or c in "-_." else "_" for c in s])
        return safe

    # context manager support
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


# ========== 示例用法 ==========
if __name__ == "__main__":
    # 直接跑测试（注意填环境变量或修改下面参数）
    fetcher = FTPFetcher(
        host=FTP_HOST,
        port=FTP_PORT,
        user=FTP_USER,
        password=FTP_PASS,
        root=FTP_ROOT,
        local_workdir=LOCAL_WORKDIR,
        processed_db_path=PROCESSED_DB,
    )
    try:
        fetcher.connect()
        pairs = fetcher.fetch_new_pairs()
        # pairs 里是下载好的本地文件信息，后续你把每个 pair 交给你的解析+kafka 上传流程
        for p in pairs:
            logger.info("本地文件 pair: %s", p)
            # EXAMPLE:
            # parse_and_upload_to_kafka(p['local_step'], p['local_record'])
            # 如果上传成功：
            # fetcher.mark_processed(p['pair_key'], p['meta_str'])
            # fetcher.cleanup_local([os.path.dirname(p['local_step'])])
    finally:
        fetcher.close()
