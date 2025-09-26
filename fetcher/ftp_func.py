import ftplib
import posixpath
from typing import List
import logging

logger = logging.getLogger(__name__)

def ftp_list_recursive(ftp: ftplib.FTP, root: str = ".", debug: bool = False) -> List[str]:
    """
    递归列出 FTP `root` 下所有文件（返回 posix 路径列表，相对于 FTP 登录目录）。
    优先使用 MLSD -> LIST -> NLST+cwd 三种策略作为降级路径。
    debug=True 时会输出诊断日志。
    """
    files = []

    def _log(*args):
        if debug:
            logger.debug(" ".join(map(str, args)))

    # 规范 root: 空字符串等同于 "."
    if not root:
        root = "."

    # helper: safe cwd and restore
    def safe_cwd(target):
        cur = None
        try:
            cur = ftp.pwd()
        except Exception:
            cur = None
        try:
            ftp.cwd(target)
            return cur, True
        except Exception as e:
            # can't cwd
            _log("safe_cwd: cwd failed", target, e)
            return cur, False

    # --- 1) MLSD 方法（若服务器支持） ---
    def walk_mlsd(path):
        try:
            # mlsd may raise if not supported
            for name, facts in ftp.mlsd(path):
                full = posixpath.join(path, name) if path not in ('.', '') else name
                typ = facts.get("type", "")
                if typ == "dir":
                    walk_mlsd(full)
                elif typ == "file" or typ == "":
                    files.append(full)
                else:
                    # unknown -> treat as file
                    files.append(full)
            return True
        except (ftplib.error_perm, AttributeError) as e:
            _log("mlsd not usable for", path, ":", e)
            return False
        except Exception as e:
            _log("mlsd unexpected error:", e)
            return False

    try:
        # 尝试 MLSD
        if walk_mlsd(root):
            # 去重、排序后返回
            normalized = sorted(set(files))
            return normalized
    except Exception as e:
        _log("mlsd top-level error:", e)

    # --- 2) LIST 方法 (解析 Unix 风格 LIST) ---
    # LIST 的输出行通常类似 "-rw-r--r-- 1 owner group  1234 Jan 01 00:00 filename"
    # 以首字符 d 表示目录。
    def walk_list(path):
        try:
            # 保存 cwd
            cur = ftp.pwd()
        except Exception:
            cur = None

        def _list_dir(p):
            try:
                # 获取 LIST 输出
                lines = []
                def callback(line):
                    lines.append(line)
                # 使用 "LIST <path>" 来列出该目录
                try:
                    ftp.retrlines("LIST " + p, callback)
                except ftplib.error_perm as e:
                    # 如果 LIST path 失败，可能 path 是文件，抬出以便外层处理
                    raise

                for line in lines:
                    # 尝试解析 Unix ls 格式
                    parts = line.split()
                    if len(parts) < 9:
                        # 无法解析，跳过或尝试通过最后一段处理
                        name = line.split()[-1]
                    else:
                        name = " ".join(parts[8:])

                    # 判断类型：第一个字符为 'd' 则为目录
                    first_char = parts[0][0] if parts and len(parts[0]) > 0 else "-"
                    full = posixpath.join(p, name) if p not in ('.','') else name
                    if first_char == 'd':
                        _list_dir(full)
                    else:
                        files.append(full)
                return True
            except ftplib.error_perm as e:
                _log("LIST failed (maybe file):", p, e)
                # 将其作为文件处理（外层会把 p 当文件加入）
                files.append(p)
                return True
            except Exception as e:
                _log("LIST unexpected error for", p, e)
                return False
            finally:
                # 恢复 cwd 到最初
                try:
                    if cur is not None:
                        ftp.cwd(cur)
                except Exception:
                    pass

        return _list_dir(path)

    try:
        if walk_list(root):
            normalized = sorted(set(files))
            return normalized
    except Exception as e:
        _log("walk_list top-level error:", e)

    # --- 3) 最后降级：nlst + cwd 探测 ---
    def walk_nlst(path):
        # 保存并切换到 path，若不能切换，则可能 path 为文件 -> 直接添加
        cur = None
        try:
            cur = ftp.pwd()
        except Exception:
            cur = None

        try:
            ftp.cwd(path)
        except Exception as e:
            _log("walk_nlst: cannot cwd into path, treat as file:", path, e)
            files.append(path)
            # 恢复 cwd
            try:
                if cur is not None:
                    ftp.cwd(cur)
            except Exception:
                pass
            return

        # 如果能 cwd，则列出当前目录的名字（不带路径）
        try:
            entries = ftp.nlst()
        except ftplib.error_perm as e:
            _log("nlst failed for", path, e)
            # restore cwd
            try:
                if cur is not None:
                    ftp.cwd(cur)
            except Exception:
                pass
            return
        except Exception as e:
            _log("nlst unexpected error for", path, e)
            try:
                if cur is not None:
                    ftp.cwd(cur)
            except Exception:
                pass
            return

        # 处理 entries（这些 name 通常是相对于 path 的 basename）
        for name in entries:
            if name in ('.', '..'):
                continue
            # build full path
            full = posixpath.join(path, name) if path not in ('.','') else name
            # try cwd into it to see if it's a dir
            try:
                ftp.cwd(name)  # since we are in path, try relative cwd
                # it is a directory; go back and recurse
                try:
                    ftp.cwd(cur if cur is not None else ".")
                except Exception:
                    pass
                walk_nlst(full)
            except ftplib.error_perm:
                # not a dir -> treat as file
                files.append(full)
            except Exception as e:
                # 不同服务器行为差异较大，尝试替代检测：尝试 SIZE（有的服务器支持）
                try:
                    size = ftp.size(full)
                    # size succeeded -> file
                    files.append(full)
                except Exception:
                    # 还是无法判断，保守地把它作为文件
                    files.append(full)

        # restore cwd to cur
        try:
            if cur is not None:
                ftp.cwd(cur)
        except Exception:
            pass

    try:
        walk_nlst(root)
    except Exception as e:
        _log("walk_nlst top-level error:", e)

    # 去重 & 排序
    normalized = sorted(set(files))
    return normalized
