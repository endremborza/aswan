import os

from ..config_class import AswanConfig, EnvConfig
from .core import Project


def project_from_dir(dirpath=None, **config_kwargs):
    return Project(
        AswanConfig.default_from_dir(dirpath or os.getcwd(), **config_kwargs)
    )


def project_from_prod_conf(prodenvconf: EnvConfig, dirpath=None):
    return Project(
        AswanConfig.default_from_dir(dirpath, prod_config=prodenvconf)
    )


def project_from_prod_info(
    prod_db: str = None,
    prod_object_store: str = None,
    prod_t2_root: str = None,
    dirpath=None,
):
    return Project(
        AswanConfig.default_from_dir(
            dirpath,
            prod_object_store=prod_object_store,
            prod_t2_root=prod_t2_root,
            prod_db=prod_db,
        )
    )
