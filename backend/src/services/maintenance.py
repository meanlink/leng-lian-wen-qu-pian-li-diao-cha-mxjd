"""运行状态：数据代际、维护锁与写入门禁。"""
import time

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from src.models import SystemState


def ensure_state(db):
    """确保单行状态存在；并发创建时退化为读取已有行。"""
    state = db.get(SystemState, 1)
    if state is not None:
        return state
    try:
        state = SystemState(id=1, generation=1, maintenance=False, backup_active=False, updated_at=time.time())
        db.add(state)
        db.flush()
        return state
    except IntegrityError:
        db.rollback()
        state = db.get(SystemState, 1)
        if state is None:
            raise
        return state


def lock_state(db):
    """在事务内锁住状态行，串行化切换、备份与后台写入。"""
    state = db.scalar(select(SystemState).where(SystemState.id == 1).with_for_update())
    if state is None:
        state = ensure_state(db)
    return state


def read_generation(db):
    # 用普通读取即可：调用方随后写回前会用 generation_matches 做锁定读复核。
    state = db.get(SystemState, 1)
    return state.generation if state else 1


def maintenance_active(db):
    state = db.get(SystemState, 1)
    return bool(state and state.maintenance)


def generation_matches(db, generation):
    """加锁读取最新代际（锁定读绕过 REPEATABLE READ 旧快照）。

    与切换事务的维护锁互斥：切换前的旧计算在切换后写入时会被识别并丢弃。
    """
    state = db.scalar(select(SystemState).where(SystemState.id == 1).with_for_update())
    return bool(state and state.generation == generation and not state.maintenance)


def claim_write_slot(db):
    """后台写事务入口：取得维护锁并确认不在恢复中。"""
    state = db.scalar(select(SystemState).where(SystemState.id == 1).with_for_update())
    if state is None:
        state = ensure_state(db)
    return not state.maintenance


def write_blocked(db):
    return maintenance_active(db)


def status_payload(db):
    state = ensure_state(db)
    return {'generation': state.generation, 'maintenance': bool(state.maintenance),
            'maintenance_reason': state.maintenance_reason or '', 'active_restore_id': state.active_restore_id,
            'backup_active': bool(state.backup_active)}


def deny_writes(db):
    """写路由的统一前置检查：取维护锁，恢复切换期间拒绝写入。

    锁持有到本请求事务提交，因此“检查通过”的写入与恢复切换不会交错；
    恢复开始后到达的写入会等待切换提交，然后被明确拒绝。
    """
    state = db.scalar(select(SystemState).where(SystemState.id == 1).with_for_update())
    if state and state.maintenance:
        db.rollback()
        raise HTTPException(503, state.maintenance_reason or '数据恢复进行中，业务写入已暂停')
    return state
