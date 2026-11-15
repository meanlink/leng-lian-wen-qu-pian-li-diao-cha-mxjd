from sqlalchemy import String, JSON, Double, Integer, Boolean, ForeignKey, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class SystemState(Base):
    """单行运行状态：数据代际与维护锁。

    generation 每次成功切换（恢复/回滚）自增；worker 用它识别晚到的旧计算。
    """
    __tablename__ = 'system_state'
    id: Mapped[int] = mapped_column(primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, default=1)
    maintenance: Mapped[bool] = mapped_column(Boolean, default=False)
    maintenance_reason: Mapped[str] = mapped_column(String(200), default='')
    active_restore_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    backup_active: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[float] = mapped_column(Double, default=0)


class Backup(Base):
    """整库备份包。业务表快照 + 归档文件 + 清单指纹。"""
    __tablename__ = 'backups'
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    note: Mapped[str] = mapped_column(String(500), default='')
    status: Mapped[str] = mapped_column(String(20), default='creating')
    path: Mapped[str] = mapped_column(String(300), default='')
    created_by: Mapped[str] = mapped_column(String(150))
    created_at: Mapped[float] = mapped_column(Double)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    generation: Mapped[int] = mapped_column(Integer, default=0)
    row_counts: Mapped[dict] = mapped_column(JSON, default=dict)
    row_total: Mapped[int] = mapped_column(Integer, default=0)
    artifact_count: Mapped[int] = mapped_column(Integer, default=0)
    size_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    package_sha256: Mapped[str] = mapped_column(String(64), default='')
    error: Mapped[str] = mapped_column(String(2000), default='')
    verification: Mapped[dict] = mapped_column(JSON, default=dict)
    internal: Mapped[bool] = mapped_column(Boolean, default=False)


class Restore(Base):
    """一次原位恢复。阶段检查点让中断后的恢复可以继续。"""
    __tablename__ = 'restores'
    id: Mapped[int] = mapped_column(primary_key=True)
    backup_id: Mapped[int] = mapped_column(ForeignKey('backups.id'))
    mode: Mapped[str] = mapped_column(String(20), default='restore')
    status: Mapped[str] = mapped_column(String(20), default='pending')
    stage: Mapped[str] = mapped_column(String(30), default='verify')
    progress: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str] = mapped_column(String(500), default='')
    error: Mapped[str] = mapped_column(String(2000), default='')
    note: Mapped[str] = mapped_column(String(500), default='')
    verification: Mapped[dict] = mapped_column(JSON, default=dict)
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    rollback_backup_id: Mapped[int | None] = mapped_column(ForeignKey('backups.id'), nullable=True)
    rollback_available: Mapped[bool] = mapped_column(Boolean, default=False)
    attempt: Mapped[int] = mapped_column(Integer, default=1)
    created_by: Mapped[str] = mapped_column(String(150))
    created_at: Mapped[float] = mapped_column(Double)
    started_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
