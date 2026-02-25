import datetime
import ftplib
import posixpath
from typing import Dict, List, Optional, Tuple

import logging

logger = logging.getLogger(__name__)

def _path_segment_count(p: str) -> int:
    norm = posixpath.normpath(p)
    parts = [seg for seg in norm.split('/') if seg]
    return len(parts)

def _list_files_in_dir(ftp: ftplib.FTP, dirpath: str) -> List[str]:
    files: List[str] = []
    dirpath = dirpath.rstrip('/')
    if dirpath == '':
        dirpath = '/'

    try:
        entries = list(ftp.mlsd(dirpath))
        for name, facts in entries:
            typ = facts.get("type", "")
            full = posixpath.join(dirpath, name)
            if typ == "file" or typ == "cdir" or typ == "":
                if typ == "dir":
                    continue
                files.append(full)
            else:
                if typ != "dir":
                    files.append(full)
        return sorted(set(files))
    except Exception:
        pass

    try:
        entries = ftp.nlst(dirpath)
    except ftplib.error_perm as e:
        logger.debug("nlst on %s failed: %s", dirpath, e)
        return []
    except Exception as e:
        logger.debug("nlst on %s unexpected error: %s", dirpath, e)
        return []

    try:
        cur = ftp.pwd()
    except Exception:
        cur = None

    for ent in entries:
        if ent.startswith(dirpath):
            candidate = ent
        else:
            candidate = posixpath.join(dirpath, posixpath.basename(ent))

        bn = posixpath.basename(candidate)
        if bn in ('.', '..', ''):
            continue

        is_file = False
        try:
            sz = ftp.size(candidate)
            if sz is not None:
                is_file = True
        except Exception:
            try:
                ftp.cwd(candidate)
                if cur is not None:
                    try:
                        ftp.cwd(cur)
                    except Exception:
                        pass
                is_file = False
            except ftplib.error_perm:
                is_file = True
            except Exception:
                is_file = True

        if is_file:
            files.append(candidate)

    if cur is not None:
        try:
            ftp.cwd(cur)
        except Exception:
            pass

    return sorted(set(files))


def list_files_for_date(ftp: ftplib.FTP, base_root: str, day: Optional[datetime.date] = None,
                        branch_max: int = 14, debug: bool = False) -> List[str]:
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
        candidate_backup = posixpath.join(base_root, branch, date_str)#, "备份")
        try:
            ftp.cwd(candidate_backup)
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

        try:
            part = _list_files_in_dir(ftp, candidate_backup)
            if part:
                logger.info("在 %s 下找到 %d 个备份文件", candidate_backup, len(part))
                found_files.extend(part)
            else:
                logger.debug("在 %s 下未找到备份文件", candidate_backup)
        except Exception as e:
            logger.exception("列出 %s 下文件时出错: %s", candidate_backup, e)

    if cur_pwd is not None:
        try:
            ftp.cwd(cur_pwd)
        except Exception:
            pass

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
