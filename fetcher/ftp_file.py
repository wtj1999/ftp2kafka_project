import datetime
import ftplib
import posixpath
from typing import Dict, List, Optional, Tuple
from fetcher.ftp_func import ftp_list_recursive  # 如果不再使用，可删除导入

import logging

logger = logging.getLogger(__name__)

def _path_segment_count(p: str) -> int:
    """返回路径的段数（忽略空段）"""
    norm = posixpath.normpath(p)
    parts = [seg for seg in norm.split('/') if seg]
    return len(parts)

def _list_files_in_dir(ftp: ftplib.FTP, dirpath: str) -> List[str]:
    """
    列出 FTP 上指定目录 dirpath 下的**直接文件**（不递归子目录）。
    返回完整的 posix 路径列表（例如 /data/.../.../备份/filename.csv）。
    优先使用 MLSD（如果服务器支持），否则用 NLST 并尝试区分文件/目录。
    """
    files: List[str] = []
    # 规范化 dirpath（去掉尾斜杠，保证非空）
    dirpath = dirpath.rstrip('/')
    if dirpath == '':
        dirpath = '/'

    # Try MLSD first
    try:
        # ftp.mlsd may raise if not supported
        entries = list(ftp.mlsd(dirpath))
        for name, facts in entries:
            typ = facts.get("type", "")
            full = posixpath.join(dirpath, name)
            if typ == "file" or typ == "cdir" or typ == "":
                # treat file-like as file (cdir/"" sometimes returned by servers; be conservative)
                # but ensure we're not listing directories; if facts say dir, skip
                if typ == "dir":
                    continue
                files.append(full)
            else:
                # if other types, attempt to include unless it's explicitly "dir"
                if typ != "dir":
                    files.append(full)
        # normalize unique
        return sorted(set(files))
    except Exception:
        # MLSD not supported or failed -> fallback to NLST approach
        pass

    # Fallback: use NLST to list names, and distinguish files by trying to cwd into them or using size()
    try:
        entries = ftp.nlst(dirpath)
    except ftplib.error_perm as e:
        # 550 often when dir missing or permission denied
        logger.debug("nlst on %s failed: %s", dirpath, e)
        return []
    except Exception as e:
        logger.debug("nlst on %s unexpected error: %s", dirpath, e)
        return []

    # Keep original cwd to restore later
    try:
        cur = ftp.pwd()
    except Exception:
        cur = None

    for ent in entries:
        # nlst may return full path or basename; build candidate full path
        if ent.startswith(dirpath):
            candidate = ent
        else:
            candidate = posixpath.join(dirpath, posixpath.basename(ent))

        # skip '.' '..'
        bn = posixpath.basename(candidate)
        if bn in ('.', '..', ''):
            continue

        # Distinguish file vs dir:
        is_file = False
        try:
            # prefer size() if server supports it: size raises on directories
            sz = ftp.size(candidate)
            if sz is not None:
                is_file = True
        except Exception:
            # size failed -> try cwd to see if it's a directory
            try:
                ftp.cwd(candidate)
                # if cwd succeeded, it's a directory -> not a file
                # restore cwd
                if cur is not None:
                    try:
                        ftp.cwd(cur)
                    except Exception:
                        pass
                is_file = False
            except ftplib.error_perm:
                # cwd failed with permission -> likely a file
                is_file = True
            except Exception:
                # network error: conservatively treat as file (so we won't miss)
                is_file = True

        if is_file:
            files.append(candidate)

    # restore cwd
    if cur is not None:
        try:
            ftp.cwd(cur)
        except Exception:
            pass

    # deduplicate & sort
    return sorted(set(files))


def list_files_for_date(ftp: ftplib.FTP, base_root: str, day: Optional[datetime.date] = None,
                        branch_max: int = 14, debug: bool = False) -> List[str]:
    """
    只扫描 base_root 下的子分支 01..{branch_max:02d}，并且只扫描指定日期目录（默认今天）之下的 '备份' 目录。
    例如 base_root=/data/ftp/upload/pack/电测 -> 会尝试 /data/.../电测/01/2025-09-25/备份, /02/2025-09-25/备份 ...
    只列出各备份目录下的**直接文件**（不递归子目录）。
    对每个 basename 若有多条路径，按：路径段数更少 -> 路径更短 -> 字典序，选择最佳一条。
    """
    if day is None:
        day = datetime.date.today()
    date_str = day.strftime("%Y-%m-%d")

    base_root = base_root.rstrip('/')
    logger.info("按日期扫描 FTP 的备份目录: base=%s date=%s branches=01..%02d", base_root, date_str, branch_max)

    found_files: List[str] = []
    try:
        cur_pwd = ftp.pwd()
    except Exception:
        cur_pwd = None

    for i in range(1, branch_max + 1):
        branch = f"{i:02d}"
        candidate_backup = posixpath.join(base_root, branch, date_str, "备份")
        # 检查备份目录是否存在（用 cwd 验证）
        try:
            ftp.cwd(candidate_backup)
            # restore cwd immediately
            if cur_pwd is not None:
                try:
                    ftp.cwd(cur_pwd)
                except Exception:
                    pass
        except ftplib.error_perm as e:
            logger.debug("远程备份路径不存在或无权限: %s (%s)", candidate_backup, e)
            continue
        except Exception as e:
            logger.debug("检查远程备份路径时出错: %s (%s)", candidate_backup, e)
            continue

        # 仅列出该备份目录下的直接文件（不递归）
        try:
            part = _list_files_in_dir(ftp, candidate_backup)
            if part:
                logger.info("在 %s 下找到 %d 个备份文件", candidate_backup, len(part))
                found_files.extend(part)
            else:
                logger.debug("在 %s 下未找到备份文件", candidate_backup)
        except Exception as e:
            logger.exception("列出 %s 下文件时出错: %s", candidate_backup, e)

    # restore cwd if needed
    if cur_pwd is not None:
        try:
            ftp.cwd(cur_pwd)
        except Exception:
            pass

    # 去重规则：按 basename 聚合，只保留每个 basename 的“最佳”路径（路径段数少 -> 更短字符串 -> 字典序）
    best_for_basename: Dict[str, str] = {}
    for p in found_files:
        norm_p = posixpath.normpath(p)
        bn = posixpath.basename(norm_p)
        if not bn:
            bn = norm_p

        if bn not in best_for_basename:
            best_for_basename[bn] = norm_p
            if debug:
                logger.debug("选择初始候选: basename=%s -> %s", bn, norm_p)
        else:
            cur_best = best_for_basename[bn]
            cur_cnt = _path_segment_count(cur_best)
            new_cnt = _path_segment_count(norm_p)
            replace = False
            if new_cnt < cur_cnt:
                replace = True
            elif new_cnt == cur_cnt:
                if len(norm_p) < len(cur_best):
                    replace = True
                elif len(norm_p) == len(cur_best):
                    if norm_p < cur_best:
                        replace = True
            if replace:
                if debug:
                    logger.debug(
                        "basename=%s 替换候选: %s (cnt=%d,len=%d) <- %s (cnt=%d,len=%d)",
                        bn, cur_best, cur_cnt, len(cur_best), norm_p, new_cnt, len(norm_p)
                    )
                best_for_basename[bn] = norm_p

    result = sorted(set(best_for_basename.values()), key=lambda x: (_path_segment_count(x), len(x), x))
    logger.info("总共找到 %d 个远程备份文件路径（按 basename 去重后）", len(result))
    if debug:
        for f in result:
            logger.debug("kept_remote_file: %s", f)
    return result
