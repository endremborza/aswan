import tarfile

import pandas as pd
import sqlalchemy as db

from aswan import AswanConfig, ProdConfig, Project
from aswan.migrate import pull, push
from aswan.models import Base
from aswan.object_store import get_object_store


def test_push_pull(tmp_path):

    conf = ProdConfig.from_dir(tmp_path / "cfg")
    Base.metadata.create_all(db.create_engine(conf.db))
    ostore = get_object_store(conf.object_store)
    remote = tmp_path / "remote"

    df1 = pd.DataFrame([{"A": 10}])
    df2 = pd.DataFrame([{"B": 10}])

    tabfp = conf.t2_path / "tab"

    df1.to_parquet(tabfp)
    ostore.dump_str("YAAAY", "fing")
    push(conf, str(remote))

    df2.to_parquet(tabfp)

    tfile = next(remote.glob("**/*.tgz"))
    with tarfile.open(tfile, "r:gz") as tar:
        names = tar.getnames()
    assert "fing" in names

    assert not pd.read_parquet(tabfp).equals(df1)
    pull(conf, str(remote))
    assert pd.read_parquet(tabfp).equals(df1)


def test_project_push_pull(tmp_path):
    aconf = AswanConfig.default_from_dir(
        tmp_path / "cfg", remote_root=str(tmp_path / "remote")
    )
    project = Project(aconf)
    project.push()
    project.pull()
