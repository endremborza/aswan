from dataclasses import asdict

from aswan import (
    AswanConfig,
    ProdConfig,
    project_from_dir,
    project_from_prod_conf,
    project_from_prod_info,
)
from aswan.constants import Envs


def test_from_dir(tmp_path):
    conf = AswanConfig.default_from_dir(tmp_path, remote_root="/remote")
    project = project_from_dir(tmp_path, remote_root="/remote")
    assert conf == project.config

    saved_to = tmp_path / "save_loc"
    saved_to.mkdir()
    conf.save(saved_to)
    assert conf.test == AswanConfig.load(saved_to).test
    assert conf.exp == AswanConfig.load(saved_to).exp
    assert conf.prod == ProdConfig(**asdict(AswanConfig.load(saved_to).prod))
    assert conf.remote_root == "/remote"
    assert conf.remote_root == AswanConfig.load(saved_to).remote_root


def test_partial(tmp_path):

    prod_t2 = tmp_path / "other_t2"

    conf = AswanConfig.default_from_dir(tmp_path, prod_t2_root=prod_t2)
    project = project_from_prod_info(dirpath=tmp_path, prod_t2_root=prod_t2)
    assert conf == project.config
    assert conf.prod.t2_root == str(prod_t2)


def test_prodconf(tmp_path):

    prod_conf = ProdConfig.from_dir(tmp_path / "other_prod")

    conf = AswanConfig.default_from_dir(tmp_path)
    conf.prod = prod_conf

    project = project_from_prod_conf(dirpath=tmp_path, prodenvconf=prod_conf)

    assert conf == project.config
    assert project.config.env_dict()[Envs.PROD] == prod_conf
