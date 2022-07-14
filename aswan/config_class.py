import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

import sqlalchemy as db
import yaml
from parquetranger import TableRepo

from .metadata_handling import MetaHandler
from .models import Base
from .object_store import ObjectStore

CONFIG_FILE = Path("aswan-config.yaml")
DEFAULT_REMOTE_SPACE = Path.home() / "aswan-data"
DEFAULT_LOCAL_SPACE = Path.home() / "aswan-raw"
DEFAULT_BATCH_SIZE = 10
DEFAULT_DIST_API = "sync"

PROD_DEFAULTS = dict(
    distributed_api="mp", min_queue_size=20, batch_size=40, keep_running=True
)

_dbprefix = "sqlite:///"


@dataclass
class EnvConfig:
    root: Path

    distributed_api: str = DEFAULT_DIST_API
    min_queue_size: int = 0
    batch_size: int = DEFAULT_BATCH_SIZE
    keep_running: bool = False

    def __post_init__(self):
        self.root.mkdir(exist_ok=True, parents=True)

    def get_engine(self):
        _engine = db.create_engine(f"{_dbprefix}{self.db_path}")
        Base.metadata.create_all(_engine)
        return _engine

    @property
    def object_store(self):
        return ObjectStore(self.root / "object_store")

    @property
    def t2_path(self):
        return Path(self.root / "t2")

    @property
    def db_path(self):
        return self.root / "db.sqlite"

    @property
    def name(self):
        return self.root.name


class AswanConfig:

    prod_name = "prod"
    exp_name = "exp"
    test_name = "test"

    def __init__(self, name: str) -> None:

        if CONFIG_FILE.exists():
            _kwargs = yaml.safe_load(CONFIG_FILE.read_text())
        else:
            _kwargs = {}

        _root = Path(_kwargs.get("local_root", DEFAULT_LOCAL_SPACE / name))
        remote_root = _kwargs.get("remote_root", DEFAULT_REMOTE_SPACE / name)
        self.remote = _Remote(remote_root)
        self.local_root = _root
        _prod_kwargs = _kwargs.get(self.prod_name, PROD_DEFAULTS)
        self.prod = EnvConfig(_root / self.prod_name, **_prod_kwargs)
        self.exp = EnvConfig(_root / self.exp_name, **_kwargs.get(self.exp_name, {}))
        self.test = EnvConfig(_root / self.test_name, **_kwargs.get(self.test_name, {}))

    def get_prod_table(self, tabname, group_cols=None):
        return TableRepo(
            self.prod.t2_path / tabname,
            env_parents=self.t2_root_dic,
            group_cols=group_cols,
        )

    def push(self, force=False, clean_ostore=False):
        force or MetaHandler(self).validate_push(self.remote.get_engine())
        conf = self.prod
        with tarfile.open(self.remote.ostore_tar, "w:gz") as tar:
            for local_abs_path in conf.object_store.root_path.iterdir():
                tar.add(local_abs_path, arcname=local_abs_path.name)

        shutil.copytree(conf.t2_path, self.remote.t2_root, dirs_exist_ok=True)
        shutil.copyfile(conf.db_path, self.remote.db_path)
        if clean_ostore:
            conf.object_store.purge()

    def pull(self, force=False, pull_ostore=False):
        force or MetaHandler(self).validate_pull(self.remote.get_engine())
        conf = self.prod
        shutil.copytree(self.remote.t2_root, conf.t2_path, dirs_exist_ok=True)
        shutil.copyfile(self.remote.db_path, conf.db_path)
        if pull_ostore:
            with tarfile.open(self.remote.ostore_tar, "r:gz") as tar:
                tar.extractall(conf)

    def purge(self, remote=False):
        shutil.rmtree(self.local_root)
        if remote:
            self.remote.purge()

    @property
    def t2_root_dic(self):
        return {k: v.t2_path for k, v in self.env_dict.items()}

    @property
    def env_dict(self) -> Dict[str, EnvConfig]:
        return {e.name: e for e in [self.prod, self.exp, self.test]}


class _Remote:
    """maybe gzip the db too, can be reduced to ~18%"""

    def __init__(self, root: str):
        # TODO make it work with s3 root.startswith("s3://")
        rootp = Path(root)
        self.ostore_tar = rootp / "objects.tgz"
        self.db_path = rootp / "db.sqlite"
        self.t2_root = rootp / "t2"
        self.t2_root.mkdir(exist_ok=True, parents=True)
        self.root = rootp

    def get_engine(self):
        return db.create_engine(f"{_dbprefix}{self.db_path.as_posix()}")

    def purge(self):
        if self.root.exists():
            shutil.rmtree(self.root)
