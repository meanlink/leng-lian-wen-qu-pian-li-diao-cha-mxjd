import pathlib, sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from src.config.database import Session
from src.models import Backup, Restore
from src.services import maintenance, ops


def test_cleanup_keeps_recent_snapshot_and_prunes_old(tmp_path):
    with Session() as db:
        maintenance.ensure_state(db)
        old = Backup(name='恢复前快照 #1', status='completed', path='', created_by='a', created_at=1,
                     generation=1, row_counts={}, row_total=0, artifact_count=0, size_bytes=0,
                     package_sha256='', error='', internal=True, verification={})
        new = Backup(name='恢复前快照 #12', status='completed', path='', created_by='a', created_at=2,
                     generation=1, row_counts={}, row_total=0, artifact_count=0, size_bytes=0,
                     package_sha256='', error='', internal=True, verification={})
        db.add_all([old, new])
        db.flush()
        kept = Backup(name='公开备份', status='completed', path='', created_by='a', created_at=3,
                      generation=1, row_counts={}, row_total=0, artifact_count=0, size_bytes=0,
                      package_sha256='', error='', internal=False, verification={})
        db.add(kept)
        db.flush()
        # 最近的恢复引用 new 作为回退快照
        db.add(Restore(backup_id=kept.id, rollback_backup_id=new.id, mode='restore', status='completed', stage='finalize',
                       progress=100, message='', error='', note='', verification={}, result={}, rollback_available=True,
                       attempt=1, created_by='a', created_at=4, started_at=4, finished_at=5))
        db.commit()
        old_id, new_id, kept_id = old.id, new.id, kept.id
    for backup_id in (old_id, new_id, kept_id):
        ops.package_dir(backup_id).mkdir(parents=True, exist_ok=True)
        (ops.package_dir(backup_id) / 'manifest.json').write_text('{}')
    (ops.backup_root() / '_tmp').mkdir(parents=True, exist_ok=True)
    ops.cleanup_workspace()
    with Session() as db:
        assert db.get(Backup, old_id).status == 'pruned'
        assert db.get(Backup, new_id).status == 'completed'
        assert db.get(Backup, kept_id).status == 'completed'
    assert not ops.package_dir(old_id).exists()
    assert ops.package_dir(new_id).exists()
    assert ops.package_dir(kept_id).exists()
    assert not (ops.backup_root() / '_tmp').exists()
