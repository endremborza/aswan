import os
from contextlib import contextmanager


@contextmanager
def change_cwd(tmp_path):
    _orig_dir = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        yield
    finally:
        os.chdir(_orig_dir)
