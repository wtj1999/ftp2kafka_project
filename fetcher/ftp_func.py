import ftplib
import posixpath
from typing import List
import logging

logger = logging.getLogger(__name__)

def ftp_list_recursive(ftp: ftplib.FTP, root: str = ".", debug: bool = False) -> List[str]:
    files = []

    def _log(*args):
        if debug:
            logger.debug(" ".join(map(str, args)))

    if not root:
        root = "."

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

    def walk_mlsd(path):
        try:
            for name, facts in ftp.mlsd(path):
                full = posixpath.join(path, name) if path not in ('.', '') else name
                typ = facts.get("type", "")
                if typ == "dir":
                    walk_mlsd(full)
                elif typ == "file" or typ == "":
                    files.append(full)
                else:
                    files.append(full)
            return True
        except (ftplib.error_perm, AttributeError) as e:
            _log("mlsd not usable for", path, ":", e)
            return False
        except Exception as e:
            _log("mlsd unexpected error:", e)
            return False

    try:
        if walk_mlsd(root):
            normalized = sorted(set(files))
            return normalized
    except Exception as e:
        _log("mlsd top-level error:", e)

    def walk_list(path):
        try:
            cur = ftp.pwd()
        except Exception:
            cur = None

        def _list_dir(p):
            try:
                lines = []
                def callback(line):
                    lines.append(line)
                try:
                    ftp.retrlines("LIST " + p, callback)
                except ftplib.error_perm as e:
                    raise

                for line in lines:
                    parts = line.split()
                    if len(parts) < 9:
                        name = line.split()[-1]
                    else:
                        name = " ".join(parts[8:])

                    first_char = parts[0][0] if parts and len(parts[0]) > 0 else "-"
                    full = posixpath.join(p, name) if p not in ('.','') else name
                    if first_char == 'd':
                        _list_dir(full)
                    else:
                        files.append(full)
                return True
            except ftplib.error_perm as e:
                _log("LIST failed (maybe file):", p, e)
                files.append(p)
                return True
            except Exception as e:
                _log("LIST unexpected error for", p, e)
                return False
            finally:
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

    def walk_nlst(path):
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
            try:
                if cur is not None:
                    ftp.cwd(cur)
            except Exception:
                pass
            return

        try:
            entries = ftp.nlst()
        except ftplib.error_perm as e:
            _log("nlst failed for", path, e)
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

        for name in entries:
            if name in ('.', '..'):
                continue
            full = posixpath.join(path, name) if path not in ('.','') else name
            try:
                ftp.cwd(name)
                try:
                    ftp.cwd(cur if cur is not None else ".")
                except Exception:
                    pass
                walk_nlst(full)
            except ftplib.error_perm:
                files.append(full)
            except Exception as e:
                try:
                    size = ftp.size(full)
                    files.append(full)
                except Exception:
                    files.append(full)

        try:
            if cur is not None:
                ftp.cwd(cur)
        except Exception:
            pass

    try:
        walk_nlst(root)
    except Exception as e:
        _log("walk_nlst top-level error:", e)

    normalized = sorted(set(files))
    return normalized
