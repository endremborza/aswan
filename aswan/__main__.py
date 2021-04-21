import sys
from pathlib import Path

from cookiecutter.main import cookiecutter

from .config_class import AswanConfig

ENV_DIR = "aswan-envs"
TEMPLATE_REPO = "https://github.com/endremborza/raw-data-project-frame.git"


def main(
    src_root,
    remote_root=None,
):
    src_path = Path(src_root)
    src_parent = src_path.parent
    src_parent.mkdir(exist_ok=True)
    cookiecutter(
        TEMPLATE_REPO,
        no_input=True,
        output_dir=src_parent.as_posix(),
        extra_context={"project_slug": src_path.name},
        overwrite_if_exists=True,
    )

    aswan_config = AswanConfig.default_from_dir(
        src_path / ENV_DIR, remote_root=remote_root
    )
    aswan_config.save(src_root)


if __name__ == "__main__":
    if sys.argv[1] in ["--help", "-h"]:
        print("args: src root, remote root")
    main(sys.argv[1], sys.argv[2])
