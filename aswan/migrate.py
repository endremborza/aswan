import datetime
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .object_store import get_object_store

if TYPE_CHECKING:
    from aswan.config_class import ProdConfig

_dbprefix = "sqlite:///"
_tarext = ".tgz"


class _Remote:
    """maybe gzip the db too, can be reduced to ~18%"""

    def __init__(self, root: str):

        self._is_s3 = root.startswith("s3://")
        rootp = Path(root)

        self.ostore = rootp / "ostore-archive"
        self.dbfile = rootp / "db.sqlite"
        self.t2_root = rootp / "t2"

        if not self._is_s3:
            rootp.mkdir(exist_ok=True)
            self.ostore.mkdir(exist_ok=True)
            self.t2_root.mkdir(exist_ok=True)

    def push(self, conf: "ProdConfig", clean_ostore=False):
        assert conf.db.startswith(_dbprefix)

        new_tar = (
            datetime.datetime.now().isoformat().replace(".", "-").replace(":", "-")
            + _tarext
        )
        ostore = get_object_store(conf.object_store)
        with ostore.tarcontext() as tfile:
            self._copyfile(tfile, self.ostore / new_tar)

        self._copyfile(conf.db.replace(_dbprefix, ""), self.dbfile)
        self._copytree(conf.t2_root, self.t2_root)
        if clean_ostore:
            ostore.purge()

    def pull(self, conf: "ProdConfig", pull_ostore=False):
        assert conf.db.startswith(_dbprefix)
        self._copyfile(self.dbfile, conf.db.replace(_dbprefix, ""))
        self._copytree(self.t2_root, conf.t2_root)

        ostore = get_object_store(conf.object_store)
        if pull_ostore:
            for tarpath in self._archive_tars():
                ostore.dump_tar(tarpath)

    def _copyfile(self, src, dst):
        if not self._is_s3:
            shutil.copy(src, dst)

    def _copytree(self, src, dst):
        if not self._is_s3:
            shutil.copytree(src, dst, dirs_exist_ok=True)

    def _archive_tars(self):
        if not self._is_s3:
            return self.ostore.glob(f"*{_tarext}")


def push(conf: "ProdConfig", remote: str, clean_ostore=False):
    _Remote(remote).push(conf, clean_ostore)


def pull(conf: "ProdConfig", remote: str, pull_ostore=False):
    _Remote(remote).pull(conf, pull_ostore)
