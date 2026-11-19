"""在线分片拆分沙盘（shardlab）。

仅依赖 Python 标准库：SQLite 作为分片存储 + 路由表 + 迁移状态机，
http.server 提供 JSON API 与静态页面。
"""

__version__ = "1.0.0"
