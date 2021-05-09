from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional, Union

import yaml
from parquetranger import TableRepo

from .constants import DistApis, Envs

CONFIG_FILE = "aswanconfig.yaml"
DEFAULT_REMOTE = "aswan-data"
DEFAULT_PROD_BATCH_SIZE = 40
DEFAULT_PROD_MIN_QUEUE_SIZE = DEFAULT_PROD_BATCH_SIZE // 2
DEFAULT_BATCH_SIZE = 10


@dataclass
class EnvConfig:
    db: str
    object_store: str
    t2_root: str

    distributed_api: str = DistApis.default()
    min_queue_size: int = 0
    batch_size: int = DEFAULT_BATCH_SIZE

    keep_running: bool = False

    @classmethod
    def from_dir(cls, envp: Union[Path, str, None] = None, **kwargs):
        envp = Path(envp if envp is not None else Path.cwd())
        t2p = Path(kwargs.get("t2_root") or envp / "t2")
        t2p.mkdir(parents=True, exist_ok=True)
        osp = Path(kwargs.get("object_store") or envp / "object_store")
        osp.mkdir(parents=True, exist_ok=True)
        dkey = "distributed_api"
        dist_api = kwargs.get(dkey, cls.__dataclass_fields__[dkey].default)
        return cls(
            db=kwargs.get("db") or f"sqlite:///{envp}/db.sqlite",
            object_store=osp.as_posix(),
            t2_root=t2p.as_posix(),
            distributed_api=dist_api,
        )

    @property
    def t2_path(self):
        return Path(self.t2_root)


@dataclass
class ProdConfig(EnvConfig):
    db: str
    object_store: str
    t2_root: str

    distributed_api: str = DistApis.RAY
    min_queue_size: int = DEFAULT_PROD_MIN_QUEUE_SIZE
    batch_size: int = DEFAULT_PROD_BATCH_SIZE

    keep_running: bool = True


@dataclass
class AswanConfig:
    prod: Optional[EnvConfig] = None
    exp: Optional[EnvConfig] = None
    test: Optional[EnvConfig] = None
    remote_root: str = DEFAULT_REMOTE

    def __post_init__(self):
        for env in Envs.all():
            if getattr(self, env) is not None:
                continue
            kls = _get_kls(env)
            setattr(
                self,
                env,
                kls.from_dir(Path.cwd() / env),
            )

    def __iter__(self):
        for e in self.env_dict().values():
            yield e

    def env_dict(self):
        return {Envs.PROD: self.prod, Envs.EXP: self.exp, Envs.TEST: self.test}

    def env_items(self):
        return self.env_dict().items()

    def save(self, dirpath: str):
        with Path(dirpath, CONFIG_FILE).open("w") as fp:
            yaml.dump(asdict(self), fp)

    def get_prod_table(self, tabname, group_cols=None):
        return TableRepo(
            self.prod.t2_path / tabname,
            env_parents=self.t2_root_dic,
            group_cols=group_cols,
        )

    def export_remote_trepos(self, fpath, trepos, append=False):
        flines = []
        if not append:
            flines.append("from parquetranger import TableRepo")

        for trepo in trepos:
            gcol = trepo.group_cols
            if isinstance(gcol, str):
                gcol = f'"{gcol}"'
            _fpath = f"{self.remote_root}/t2/{trepo.name}"
            flines.append(
                f'{trepo.name} = TableRepo("{_fpath}"' f", group_cols={gcol})"
            )
        Path(fpath).write_text("\n\n".join(flines))

    @classmethod
    def load(cls, dirpath: str = "."):
        with Path(dirpath, CONFIG_FILE).open() as fp:
            ydic = yaml.safe_load(fp)
        for env in Envs.all():
            ydic[env] = EnvConfig(**ydic[env])

        return cls(**ydic)

    @classmethod
    def default_from_dir(
        cls,
        dirpath: Union[str, Path, None],
        remote_root=None,
        **kwargs,
    ):
        """can use prefixes in kwargs like test_ or exp_"""

        dirpath = Path(dirpath or Path.cwd())
        env_dic = {
            "remote_root": remote_root or (dirpath / DEFAULT_REMOTE).as_posix()
        }
        for env in Envs.all():
            prefix = env + "_"
            env_conf = kwargs.get(prefix + "config")
            if env_conf is None:
                env_kwargs = {
                    k.replace(prefix, ""): v
                    for k, v in kwargs.items()
                    if k.startswith(prefix)
                }
                env_dic[env] = _get_kls(env).from_dir(
                    dirpath / env, **env_kwargs
                )
            else:
                env_dic[env] = env_conf
        return cls(**env_dic)

    @property
    def t2_root_dic(self):
        return {env: getattr(self, env).t2_path for env in Envs.all()}


def _get_kls(env):
    return EnvConfig if env != Envs.PROD else ProdConfig
