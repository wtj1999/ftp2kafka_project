import ftplib
import posixpath
import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)

def _safe_sendcmd(ftp: ftplib.FTP, cmd: str) -> Optional[str]:
    """Wrapper around sendcmd，统一捕获异常并返回 None（不会抛出）。"""
    try:
        return ftp.sendcmd(cmd)
    except ftplib.error_perm as e:
        # 常见 500/502/550 等权限或不支持情形
        logger.debug("_safe_sendcmd perm error for %s: %s", cmd, e)
        return None
    except Exception as e:
        logger.debug("_safe_sendcmd other error for %s: %s", cmd, e)
        return None

def _parse_list_for_name(lines: list, basename: str):
    """
    从 LIST 输出行中找到 basename 对应的行并解析 size（尽量）。
    返回 size:int or None, mdtm_str: str or None (格式不保证).
    """
    for line in lines:
        # 常见 unix style: "-rw-r--r-- 1 owner group  1234 Jan 01 00:00 filename"
        # windows style: "01-01-20  12:00PM       1234 filename"
        if basename in line:
            parts = re.split(r'\s+', line.strip(), maxsplit=8)
            # unix-like 尝试
            if len(parts) >= 5 and re.match(r'^\d+$', parts[4]):
                try:
                    size = int(parts[4])
                    return size, None
                except Exception:
                    pass
            # windows-like: size 在倒数第二段
            tokens = line.split()
            for t in reversed(tokens[:-1]):
                if re.match(r'^\d+$', t):
                    try:
                        return int(t), None
                    except Exception:
                        break
    return None, None

def get_remote_file_info(ftp: ftplib.FTP, path: str, debug: bool = False) -> Dict[str, Optional[str]]:
    """
    返回字典: { 'mdtm': 'YYYYMMDDhhmmss' or None, 'size': int or None }
    更稳健地尝试 MLSD -> MDTM/SIZE -> LIST。
    """
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    result = {"mdtm": None, "size": None}

    if not path:
        return result

    # 规范化 path：去掉两端空白
    path = path.strip()

    # 1) 如果支持 MLSD：通过目录 + basename 查找 facts
    try:
        dirname, basename = posixpath.split(path)
        if dirname == "":
            dirname = "."
        if hasattr(ftp, "mlsd"):
            try:
                # mlsd may raise if not supported; wrap in try
                for name, facts in ftp.mlsd(dirname):
                    if name == basename:
                        # facts 常包含 'modify' (YYYYMMDDHHMMSS) 和 'size'
                        modify = facts.get("modify") or facts.get("Modify") or facts.get("last-modified")
                        size = facts.get("size")
                        if modify:
                            result["mdtm"] = modify
                        if size is not None:
                            try:
                                result["size"] = int(size)
                            except Exception:
                                pass
                        if debug:
                            logger.debug("MLSD found: %s -> modify=%s size=%s", path, modify, size)
                        return result
            except ftplib.error_perm as e:
                # MLSD 不被支持或权限问题，继续降级
                if debug:
                    logger.debug("mlsd error for %s: %s", dirname, e)
            except Exception as e:
                if debug:
                    logger.debug("mlsd unexpected error: %s", e)
    except Exception as e:
        if debug:
            logger.debug("mlsd-check error: %s", e)

    # 2) 尝试 MDTM 和 SIZE（若服务器支持）
    try:
        resp = _safe_sendcmd(ftp, "MDTM " + path)
        if resp and resp.startswith("213"):
            # 格式: "213 20230908123456"
            parts = resp.split()
            if len(parts) >= 2:
                result["mdtm"] = parts[1].strip()
                if debug:
                    logger.debug("MDTM response for %s: %s", path, result["mdtm"])
    except Exception as e:
        if debug:
            logger.debug("MDTM failed for %s: %s", path, e)

    # SIZE
    try:
        size_val = ftp.size(path)
        if size_val is not None:
            result["size"] = int(size_val)
            if debug:
                logger.debug("SIZE response for %s: %s", path, size_val)
    except ftplib.error_perm as e:
        # SIZE 可能被拒绝或不支持
        if debug:
            logger.debug("SIZE not available for %s: %s", path, e)
    except Exception as e:
        if debug:
            logger.debug("SIZE error for %s: %s", path, e)

    # 如果通过 MDTM/SIZE 已得到至少一项，直接返回（优先返回 MDTM/SIZE 结果）
    if result["mdtm"] is not None or result["size"] is not None:
        return result

    # 3) 降级：尝试 LIST 并从输出中解析（不保证能得到 mdtm）
    try:
        dirname, basename = posixpath.split(path)
        if dirname == "":
            dirname = "."
        lines = []
        def _collect(line):
            lines.append(line)
        try:
            ftp.retrlines("LIST " + dirname, _collect)
        except ftplib.error_perm as e:
            # LIST <dirname> 可能失败（权限或 dirname 为文件）
            # 如果 dirname 看起来像文件（即 path 本身可能在根下），尝试 LIST path
            lines = []
            try:
                ftp.retrlines("LIST " + path, _collect)
            except Exception as e2:
                if debug:
                    logger.debug("LIST failed for dirname and path: %s, %s", dirname, e2)
                lines = []

        if lines:
            size_parsed, mdtm_parsed = _parse_list_for_name(lines, basename)
            if size_parsed is not None:
                result["size"] = size_parsed
            # LIST 不一定含有可解析的时间信息，故 mdtm_parsed 常为 None
            if debug:
                logger.debug("LIST parsed for %s -> size=%s", path, result["size"])
    except Exception as e:
        if debug:
            logger.debug("LIST parsing failed for %s: %s", path, e)

    return result
