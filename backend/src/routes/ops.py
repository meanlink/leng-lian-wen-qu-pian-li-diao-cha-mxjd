from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from src.config.database import get_db
from src.middlewares.auth import current_user, admin_only
from src.models import Backup, Restore
from src.services import maintenance, ops
from src.services.audit import record

router = APIRouter()


class BackupInput(BaseModel):
    name: str = Field(default='', max_length=160)
    note: str = Field(default='', max_length=500)


class RestoreInput(BaseModel):
    backup_id: int
    note: str = Field(default='', max_length=500)


@router.get('/ops/status')
def ops_status(db=Depends(get_db), u=Depends(current_user)):
    return maintenance.status_payload(db)


@router.get('/backups')
def backups(db=Depends(get_db), u=Depends(current_user)):
    return [ops.backup_dict(b) for b in db.scalars(select(Backup).where(Backup.internal == False).order_by(Backup.id.desc()))]  # noqa: E712


@router.post('/backups', status_code=201)
def create_backup(data: BackupInput, db=Depends(get_db), u=Depends(admin_only)):
    backup = ops.create_backup(db, data.name, data.note, u.email)
    return ops.backup_dict(backup)


@router.get('/backups/{id}')
def backup_detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    backup = db.get(Backup, id)
    if not backup or backup.internal:
        raise HTTPException(404, '备份不存在')
    return ops.backup_dict(backup)


@router.post('/backups/{id}/verify')
def verify_backup(id: int, db=Depends(get_db), u=Depends(admin_only)):
    backup = db.get(Backup, id)
    if not backup or backup.internal:
        raise HTTPException(404, '备份不存在')
    report = ops.verify_backup(db, backup)
    record(db, u.email, 'backup.verify', {'backup': id, 'ok': report.get('ok')})
    db.commit()
    return report


@router.get('/restores')
def restores(db=Depends(get_db), u=Depends(current_user)):
    return [ops.restore_dict(r) for r in db.scalars(select(Restore).order_by(Restore.id.desc()).limit(50))]


@router.post('/restores', status_code=201)
def create_restore(data: RestoreInput, db=Depends(get_db), u=Depends(admin_only)):
    restore = ops.start_restore(db, data.backup_id, u.email, data.note)
    return ops.restore_dict(restore)


@router.get('/restores/{id}')
def restore_detail(id: int, db=Depends(get_db), u=Depends(current_user)):
    restore = db.get(Restore, id)
    if not restore:
        raise HTTPException(404, '恢复任务不存在')
    return ops.restore_dict(restore)


@router.post('/restores/{id}/resume')
def resume_restore(id: int, db=Depends(get_db), u=Depends(admin_only)):
    restore = db.get(Restore, id)
    if not restore:
        raise HTTPException(404, '恢复任务不存在')
    restore = ops.resume_restore(db, restore, u.email)
    return ops.restore_dict(restore)


@router.post('/restores/{id}/cancel')
def cancel_restore(id: int, db=Depends(get_db), u=Depends(admin_only)):
    restore = db.get(Restore, id)
    if not restore:
        raise HTTPException(404, '恢复任务不存在')
    restore = ops.cancel_restore(db, restore, u.email)
    return ops.restore_dict(restore)


@router.post('/restores/{id}/rollback', status_code=201)
def rollback_restore(id: int, db=Depends(get_db), u=Depends(admin_only)):
    restore = db.get(Restore, id)
    if not restore:
        raise HTTPException(404, '恢复任务不存在')
    rollback = ops.create_rollback(db, restore, u.email)
    return ops.restore_dict(rollback)
