import datetime
import ftplib
import posixpath
from typing import Dict, List, Optional, Tuple
from fetcher.ftp_func import ftp_list_recursive

import logging

logger = logging.getLogger(__name__)

def _path_segment_count(p: str) -> int:
    """返回路径的段数（忽略空段）"""
    norm = posixpath.normpath(p)
    parts = [seg for seg in norm.split('/') if seg]
    return len(parts)

def list_files_for_date(ftp: ftplib.FTP, base_root: str, day: Optional[datetime.date] = None,
                        branch_max: int = 14, debug: bool = False) -> List[str]:
    """
    只扫描 base_root 下的子分支 01..{branch_max:02d}，并且只扫描指定日期目录（默认今天）。
    例如 base_root=/data/ftp/upload/pack/电测 -> 会尝试 /data/.../电测/01/2025-09-25, /02/2025-09-25 ...
    对每个存在的路径调用 ftp_list_recursive 并汇总结果（去重、排序后返回）。

    优化：当多个路径存在相同文件名（basename）时，只保留一条 —— 优先保留路径段数更少的；
    若段数相同则选择路径字符串更短的；若仍相同则按字典序选择最小者。
    """
    if day is None:
        day = datetime.date.today()
    date_str = day.strftime("%Y-%m-%d")

    base_root = base_root.rstrip('/')
    logger.info("按日期扫描 FTP: base=%s date=%s branches=01..%02d", base_root, date_str, branch_max)

    found_files: List[str] = []
    try:
        cur_pwd = ftp.pwd()
    except Exception:
        cur_pwd = None

    for i in range(1, branch_max + 1):
        branch = f"{i:02d}"
        candidate = posixpath.join(base_root, branch, date_str)
        # 先尝试 cwd 验证路径是否存在并可访问（避免对不存在路径调用递归函数耗时）
        try:
            ftp.cwd(candidate)
            if cur_pwd is not None:
                try:
                    ftp.cwd(cur_pwd)
                except Exception:
                    # 忽略恢复失败
                    pass
        except ftplib.error_perm as e:
            logger.debug("远程路径不存在或无权限: %s (%s)", candidate, e)
            continue
        except Exception as e:
            logger.debug("检查远程路径时出错: %s (%s)", candidate, e)
            continue

        # candidate 存在 -> 递归列出其下所有文件
        try:
            part = ftp_list_recursive(ftp, candidate)
            if part:
                logger.info("在 %s 下找到 %d 个文件/路径", candidate, len(part))
                found_files.extend(part)
            else:
                logger.debug("在 %s 下未找到文件", candidate)
        except Exception as e:
            logger.exception("递归列出 %s 时出错: %s", candidate, e)

    # 恢复原始 cwd（若需要）
    if cur_pwd is not None:
        try:
            ftp.cwd(cur_pwd)
        except Exception:
            pass

    # 去重规则：按 basename 聚合，只保留每个 basename 的“最佳”路径（路径段数少 -> 更短字符串 -> 字典序）
    best_for_basename: Dict[str, str] = {}
    for p in found_files:
        # 规范化路径用于比较，但保留原路径字符串
        norm_p = posixpath.normpath(p)
        bn = posixpath.basename(norm_p)
        if not bn:
            # 路径以/结尾或异常，使用完整路径作为 key（较少情况）
            bn = norm_p

        if bn not in best_for_basename:
            best_for_basename[bn] = norm_p
            if debug:
                logger.debug("选择初始候选: basename=%s -> %s", bn, norm_p)
        else:
            cur_best = best_for_basename[bn]
            # 比较路径段数
            cur_cnt = _path_segment_count(cur_best)
            new_cnt = _path_segment_count(norm_p)
            replace = False
            if new_cnt < cur_cnt:
                replace = True
            elif new_cnt == cur_cnt:
                # 段数相同，比较字符串长度
                if len(norm_p) < len(cur_best):
                    replace = True
                elif len(norm_p) == len(cur_best):
                    # 最后按字典序（稳定选择）
                    if norm_p < cur_best:
                        replace = True
            if replace:
                if debug:
                    logger.debug(
                        "basename=%s 替换候选: %s (cnt=%d,len=%d) <- %s (cnt=%d,len=%d)",
                        bn, cur_best, cur_cnt, len(cur_best), norm_p, new_cnt, len(norm_p)
                    )
                best_for_basename[bn] = norm_p

    # 返回去重后的路径列表，排序以便输出稳定（按路径长度然后字典序）
    result = sorted(set(best_for_basename.values()), key=lambda x: (_path_segment_count(x), len(x), x))
    logger.info("总共找到 %d 个远程文件路径（按 basename 去重后）", len(result))
    if debug:
        for f in result:
            logger.debug("kept_remote_file: %s", f)
    return result
