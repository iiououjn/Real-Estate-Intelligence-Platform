import tempfile
from pathlib import Path

from dre.config import Settings


def make_settings(tmp: str, **overrides) -> Settings:
    base = dict(mode="local", data_dir=Path(tmp), warehouse="sqlite", sqlite_path=":memory:", clean_format="csv")
    base.update(overrides)
    return Settings(**base)
