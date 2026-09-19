"""运行配置，全部通过环境变量注入。"""

from __future__ import annotations

import os
from pathlib import Path


class Settings:
    """服务配置。

    DATABASE_PATH: SQLite 数据库文件位置，启动时自动建目录、自动建表。
    """

    def __init__(self) -> None:
        self.database_path = Path(os.getenv("DATABASE_PATH", "data/mailarchive.db"))
        self.app_title = "邮件归档整理服务"


settings = Settings()
