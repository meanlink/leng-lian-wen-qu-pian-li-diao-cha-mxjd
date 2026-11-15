"""整库备份与原位恢复。

备份包是目录：manifest.json + data/*.json + artifacts/*。
- 数据快照在单个数据库事务中读取；归档只捕获快照中已提交分发引用的文件，
  因此备份与运行中的计算、分发并发时，包内数据与文件仍属同一版本。
- 恢复按阶段检查点执行：核对 → 生成回退快照 → 暂存归档 → 单事务切换数据库
  → 替换归档 → 收尾。各阶段可重入，中断后可从中断点继续；重试是整表替换，
  不会重复写入。
- 切换数据库时提升 generation。worker 在计算前后各核对一次 generation
  与维护锁，晚到的旧计算结果会被丢弃，不会污染恢复结果。
"""
import hashlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import delete, insert, select

from src.config.database import Session
from src.config.settings import ARTIFACT_DIR, BACKUP_DIR
from src.models import Audit, Backup, Dataset, Delivery, Job, Report, Restore, User
from src.services import maintenance
from src.services.audit import record
from src.utils.serialization import checksum

FORMAT = 'cold-excursion-backup/1'
TABLES = [
    ('users', User, ['id', 'email', 'name', 'role', 'password']),
    ('datasets', Dataset, ['id', 'name', 'source', 'records', 'checksum', 'created_at']),
    ('jobs', Job, ['id', 'name', 'dataset_id', 'params', 'status', 'result', 'error', 'approved', 'created_by', 'created_at', 'finished_at']),
    ('reports', Report, ['id', 'name', 'job_id', 'destination', 'interval_seconds', 'next_run', 'enabled']),
    ('deliveries', Delivery, ['id', 'report_id', 'created_at', 'checksum', 'filename', 'destination']),
    ('audits', Audit, ['id', 'actor', 'action', 'detail', 'at']),
]
ACTIVE_RESTORE_STATUSES = ('pending', 'running')
BUSINESS_TABLES = {name: model for name, model, _ in TABLES}
STAGE_PROGRESS = {'verify': 5, 'snapshot': 25, 'stage': 40, 'switch': 60, 'files': 85, 'finalize': 95}


class PackageError(Exception):
    def __init__(self, message, report=None):
        super().__init__(message)
        self.report = report or {}


def backup_root():
    root = Path(BACKUP_DIR)
    root.mkdir(parents=True, exist_ok=True)
    return root


def package_dir(backup_id):
    return backup_root() / 'packages' / f'backup-{backup_id}'


def staging_dir(restore_id):
    return backup_root() / '_staging' / f'restore-{restore_id}'


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(',', ':'))


def sha_text(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def file_sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def row_dict(obj, columns):
    return {column: getattr(obj, column) for column in columns}


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    text = canonical(value)
    with open(path, 'w', encoding='utf-8') as handle:
        handle.write(text)
    return sha_text(text)


def _read_json(path):
    with open(path, 'r', encoding='utf-8') as handle:
        return json.load(handle)


def _snapshot_tables(db):
    return {name: [row_dict(obj, columns) for obj in db.scalars(select(model).order_by(model.id))]
            for name, model, columns in TABLES}


def _artifact_names(snapshot):
    """快照中已提交分发对应的归档文件（CSV 与 JSON）。"""
    names = set()
    for delivery in snapshot['deliveries']:
        filename = str(delivery.get('filename') or '')
        if not filename or '/' in filename or '\\' in filename or filename in ('.', '..'):
            raise PackageError(f'分发归档 #{delivery.get("id")} 文件名不合法，无法备份')
        names.add(filename)
        names.add(filename + '.json')
    return sorted(names)


def _package_size(path):
    total = 0
    for item in Path(path).rglob('*'):
        if item.is_file():
            total += item.stat().st_size
    return total


def capture_package(destination, meta, snapshot):
    """把快照与对应归档写入 destination（临时目录），返回清单。"""
    destination = Path(destination)
    if destination.exists():
        shutil.rmtree(destination)
    (destination / 'data').mkdir(parents=True)
    (destination / 'artifacts').mkdir(parents=True)
    table_meta = {}
    for name, _, _ in TABLES:
        rows = snapshot.get(name, [])
        table_meta[name] = {'count': len(rows), 'sha256': _write_json(destination / 'data' / f'{name}.json', rows)}
    artifact_meta = []
    for filename in _artifact_names(snapshot):
        source = Path(ARTIFACT_DIR) / filename
        if not source.is_file():
            raise PackageError(f'归档文件缺失：{filename}，无法完成整库备份')
        target = destination / 'artifacts' / filename
        shutil.copyfile(source, target)
        data = target.read_bytes()
        artifact_meta.append({'name': filename, 'sha256': hashlib.sha256(data).hexdigest(), 'size': len(data)})
    content = {
        'format': FORMAT,
        'kind': meta.get('kind', 'backup'),
        'name': meta.get('name', ''),
        'note': meta.get('note', ''),
        'created_at': meta.get('created_at', time.time()),
        'created_by': meta.get('created_by', ''),
        'generation': int(meta.get('generation') or 0),
        'tables': table_meta,
        'artifacts': artifact_meta,
    }
    content['package_sha256'] = sha_text(canonical(content))
    _write_json(destination / 'manifest.json', content)
    return content


def verify_package(path):
    """完整核对备份包。返回 (核对报告, 快照, 清单)；任何不一致抛 PackageError。"""
    try:
        return _verify_package(path)
    except PackageError:
        raise
    except Exception as error:
        # 结构畸形导致的下标/类型错误同样视为坏包，不能泄漏为 500。
        raise PackageError(f'备份包结构无法核对：{type(error).__name__}')


def _verify_package(path):
    path = Path(path)
    report = {'checked_at': time.time(), 'ok': False,
              'tables': {}, 'artifacts': {'count': 0, 'verified': 0, 'missing': [], 'mismatched': []},
              'links': {'ok': True, 'checked': 0}, 'warnings': []}
    manifest_path = path / 'manifest.json'
    if not manifest_path.is_file():
        raise PackageError('备份包缺少 manifest.json，拒绝恢复', report)
    try:
        manifest = _read_json(manifest_path)
    except (OSError, ValueError):
        raise PackageError('备份包 manifest.json 无法解析，拒绝恢复', report)
    if not isinstance(manifest, dict) or manifest.get('format') != FORMAT:
        raise PackageError('备份包格式不受支持，拒绝恢复', report)
    declared = manifest.get('package_sha256', '')
    content = {key: value for key, value in manifest.items() if key != 'package_sha256'}
    if not declared or declared != sha_text(canonical(content)):
        raise PackageError('备份包清单指纹不一致，可能已损坏或被改动，拒绝恢复', report)
    report['package_sha256'] = declared
    report['created_at'] = manifest.get('created_at')
    snapshot = {}
    for name, _, _ in TABLES:
        entry = manifest.get('tables', {}).get(name) if isinstance(manifest.get('tables'), dict) else None
        if not isinstance(entry, dict):
            raise PackageError(f'备份包缺少 {name} 数据，拒绝恢复', report)
        data_path = path / 'data' / f'{name}.json'
        if not data_path.is_file():
            raise PackageError(f'备份包缺少 {name} 数据文件，拒绝恢复', report)
        data = data_path.read_bytes()
        if hashlib.sha256(data).hexdigest() != entry.get('sha256'):
            raise PackageError(f'{name} 数据指纹不一致，拒绝恢复', report)
        try:
            rows = json.loads(data.decode('utf-8'))
        except (UnicodeDecodeError, ValueError):
            raise PackageError(f'{name} 数据无法解析，拒绝恢复', report)
        try:
            expected = int(entry.get('count', -1))
        except (TypeError, ValueError):
            raise PackageError(f'{name} 清单条数不合法，拒绝恢复', report)
        if not isinstance(rows, list) or len(rows) != expected:
            raise PackageError(f'{name} 数据条数与清单不符，拒绝恢复', report)
        snapshot[name] = rows
        report['tables'][name] = {'count': len(rows), 'expected': expected, 'ok': True}
    _verify_rows(snapshot, report)
    _verify_artifacts(path, manifest, snapshot, report)
    report['ok'] = True
    return report, snapshot, manifest


def _verify_rows(snapshot, report):
    ids = {}
    for name, _, _ in TABLES:
        seen = set()
        for row in snapshot[name]:
            if not isinstance(row, dict):
                raise PackageError(f'{name} 存在非对象记录，拒绝恢复', report)
            row_id = row.get('id')
            if not isinstance(row_id, int) or isinstance(row_id, bool) or row_id <= 0:
                raise PackageError(f'{name} 存在非法主键，拒绝恢复', report)
            if row_id in seen:
                raise PackageError(f'{name} 主键重复：{row_id}，拒绝恢复', report)
            seen.add(row_id)
        ids[name] = seen
    datasets = {int(row['id']): row for row in snapshot['datasets']}
    jobs = {int(row['id']): row for row in snapshot['jobs']}
    reports = {int(row['id']): row for row in snapshot['reports']}
    for row in snapshot['datasets']:
        try:
            if not isinstance(row.get('records'), list) or checksum(row['records']) != row.get('checksum'):
                raise ValueError()
        except (KeyError, TypeError, ValueError):
            raise PackageError(f'数据批次 #{row.get("id")} 内容指纹与记录不一致，拒绝恢复', report)
    for row in snapshot['jobs']:
        if int(row.get('dataset_id') or 0) not in datasets:
            raise PackageError(f'分析任务 #{row["id"]} 关联的数据批次不存在，拒绝恢复', report)
        if int(row.get('created_by') or 0) not in ids['users']:
            raise PackageError(f'分析任务 #{row["id"]} 关联的用户不存在，拒绝恢复', report)
        if row.get('status') not in ('queued', 'running', 'completed', 'failed'):
            raise PackageError(f'分析任务 #{row["id"]} 状态不合法，拒绝恢复', report)
        report['links']['checked'] += 1
    for row in snapshot['reports']:
        if int(row.get('job_id') or 0) not in jobs:
            raise PackageError(f'分发计划 #{row["id"]} 关联的分析任务不存在，拒绝恢复', report)
        if int(row.get('interval_seconds') or 0) < 60:
            raise PackageError(f'分发计划 #{row["id"]} 周期不合法，拒绝恢复', report)
        report['links']['checked'] += 1
    for row in snapshot['deliveries']:
        if int(row.get('report_id') or 0) not in reports:
            raise PackageError(f'归档 #{row["id"]} 关联的分发计划不存在，拒绝恢复', report)
        report['links']['checked'] += 1
    emails = [str(row.get('email') or '').lower() for row in snapshot['users']]
    if len(set(emails)) != len(emails) or '' in emails:
        raise PackageError('备份包用户邮箱不合法或重复，拒绝恢复', report)
    names = [str(row.get('name') or '') for row in snapshot['datasets']]
    if len(set(names)) != len(names) or '' in names:
        raise PackageError('备份包数据批次名称不合法或重复，拒绝恢复', report)


def _verify_artifacts(path, manifest, snapshot, report):
    listed = manifest.get('artifacts')
    if not isinstance(listed, list):
        raise PackageError('备份包缺少归档清单，拒绝恢复', report)
    expected = {}
    for item in listed:
        if not isinstance(item, dict) or not isinstance(item.get('name'), str):
            raise PackageError('备份包归档清单格式不合法，拒绝恢复', report)
        name = item['name']
        if name in expected:
            raise PackageError(f'备份包归档清单重复：{name}，拒绝恢复', report)
        if not name or '/' in name or '\\' in name or name in ('.', '..'):
            raise PackageError(f'备份包归档文件名不合法：{name}', report)
        expected[name] = item
    report['artifacts']['count'] = len(expected)
    referenced = set(_artifact_names(snapshot))
    missing_listed = referenced - set(expected)
    if missing_listed:
        raise PackageError('备份包归档清单不完整：' + '、'.join(sorted(missing_listed)[:3]) + '，拒绝恢复', report)
    unlisted = set(expected) - referenced
    if unlisted:
        report['warnings'].append('备份包含未被分发引用的归档：' + '、'.join(sorted(unlisted)[:5]))
    artifacts_dir = path / 'artifacts'
    actual = {item.name for item in artifacts_dir.iterdir() if item.is_file()} if artifacts_dir.is_dir() else set()
    extra = actual - set(expected)
    if extra:
        raise PackageError('备份包存在未登记文件：' + '、'.join(sorted(extra)[:3]) + '，拒绝恢复', report)
    for name, item in sorted(expected.items()):
        file_path = artifacts_dir / name
        if not file_path.is_file():
            report['artifacts']['missing'].append(name)
            raise PackageError(f'备份包归档文件缺失：{name}，拒绝恢复', report)
        data = file_path.read_bytes()
        if hashlib.sha256(data).hexdigest() != item.get('sha256') or len(data) != int(item.get('size', len(data))):
            report['artifacts']['mismatched'].append(name)
            raise PackageError(f'备份包归档文件指纹不一致：{name}，拒绝恢复', report)
        report['artifacts']['verified'] += 1
    for delivery in snapshot['deliveries']:
        filename = delivery.get('filename')
        if not isinstance(filename, str) or not filename:
            raise PackageError(f'归档 #{delivery.get("id")} 缺少文件名，拒绝恢复', report)
        csv_path = artifacts_dir / filename
        if not csv_path.is_file():
            raise PackageError(f'归档 #{delivery["id"]} 的 CSV 文件缺失，拒绝恢复', report)
        if file_sha(csv_path) != delivery.get('checksum'):
            raise PackageError(f'归档 #{delivery["id"]} 的 CSV 校验值与记录不一致，拒绝恢复', report)
        json_path = artifacts_dir / (filename + '.json')
        if not json_path.is_file():
            raise PackageError(f'归档 #{delivery["id"]} 的 JSON 文件缺失，拒绝恢复', report)
        try:
            payload = _read_json(json_path)
        except (OSError, ValueError):
            raise PackageError(f'归档 #{delivery["id"]} 的 JSON 无法解析，拒绝恢复', report)
        if not isinstance(payload, dict) or payload.get('checksum') != delivery.get('checksum'):
            raise PackageError(f'归档 #{delivery["id"]} 的 JSON 校验值与记录不一致，拒绝恢复', report)


# ---------------------------------------------------------------- backups

def create_backup(db, name, note, actor):
    """整库备份：先占位，再异步捕获快照与归档。"""
    state = maintenance.lock_state(db)
    if state.maintenance:
        raise HTTPException(503, state.maintenance_reason or '恢复期间不能创建备份')
    if state.backup_active:
        raise HTTPException(409, '已有整库备份正在进行，请稍后重试')
    state.backup_active = True
    state.updated_at = time.time()
    backup = Backup(name=(name or '').strip()[:160] or ('整库备份 · ' + time.strftime('%Y-%m-%d %H:%M')),
                    note=(note or '').strip()[:500], status='creating', path='', created_by=actor,
                    created_at=time.time(), generation=state.generation, row_counts={}, row_total=0,
                    artifact_count=0, size_bytes=0, package_sha256='', error='', internal=False)
    db.add(backup)
    db.flush()
    backup_id = backup.id
    record(db, actor, 'backup.start', {'backup': backup_id, 'name': backup.name})
    db.commit()
    threading.Thread(target=_run_backup, args=(backup_id,), name=f'backup-{backup_id}', daemon=True).start()
    return backup


def _run_backup(backup_id):
    with Session() as db:
        backup = db.get(Backup, backup_id)
        if not backup:
            return
        meta = {'kind': 'backup', 'name': backup.name, 'note': backup.note,
                'created_at': backup.created_at, 'created_by': backup.created_by, 'generation': backup.generation}
        # 单事务读取快照：并发提交的计算/分发要么整体进入快照，要么整体排除。
        snapshot = _snapshot_tables(db)
    tmp = backup_root() / '_tmp' / f'backup-{backup_id}-{uuid.uuid4().hex[:8]}'
    try:
        manifest = capture_package(tmp, meta, snapshot)
        final = package_dir(backup_id)
        final.parent.mkdir(parents=True, exist_ok=True)
        if final.exists():
            shutil.rmtree(final)
        os.replace(tmp, final)
        _finish_backup_row(backup_id, manifest, final)
        with Session() as db:
            record(db, 'system', 'backup.completed', {'backup': backup_id,
                                                      'rows': sum(len(rows) for rows in snapshot.values()),
                                                      'files': len(manifest['artifacts'])})
            db.commit()
    except Exception as error:
        logging.exception('Backup failed')
        shutil.rmtree(tmp, ignore_errors=True)
        with Session() as db:
            backup = db.get(Backup, backup_id)
            if backup:
                backup.status = 'failed'
                backup.error = str(error)[:1900]
                backup.finished_at = time.time()
                record(db, 'system', 'backup.failed', {'backup': backup_id, 'error': backup.error[:300]})
            db.commit()
    finally:
        with Session() as db:
            state = maintenance.lock_state(db)
            state.backup_active = False
            state.updated_at = time.time()
            db.commit()


def _finish_backup_row(backup_id, manifest, path):
    total = sum(int(item['count']) for item in manifest['tables'].values())
    with Session() as db:
        backup = db.get(Backup, backup_id)
        if not backup:
            return
        backup.status = 'completed'
        backup.path = str(path)
        backup.finished_at = time.time()
        backup.row_counts = {name: int(item['count']) for name, item in manifest['tables'].items()}
        backup.row_total = total
        # 归档数量按分发记录计；每个分发会备份 CSV 与 JSON 两个文件。
        backup.artifact_count = int(manifest['tables'].get('deliveries', {}).get('count', 0))
        backup.size_bytes = _package_size(path)
        backup.package_sha256 = manifest['package_sha256']
        backup.error = ''
        db.commit()


def verify_backup(db, backup):
    if backup.status != 'completed':
        raise HTTPException(409, '备份尚未完成，无法核对')
    try:
        report, _, _ = verify_package(package_dir(backup.id))
    except PackageError as error:
        report = dict(error.report or {})
        report.update({'ok': False, 'message': str(error)})
        backup.verification = report
        db.commit()
        raise HTTPException(422, str(error))
    backup.verification = report
    db.commit()
    return report


def create_internal_backup(restore, label, snapshot):
    """切换前自动生成的回退快照。"""
    with Session() as db:
        backup = Backup(name=f'{label} #{restore.id}', note=f'恢复任务 #{restore.id} 自动生成', status='creating',
                        path='', created_by=restore.created_by, created_at=time.time(),
                        generation=maintenance.ensure_state(db).generation, row_counts={}, row_total=0,
                        artifact_count=0, size_bytes=0, package_sha256='', error='', internal=True)
        db.add(backup)
        db.flush()
        backup_id = backup.id
        db.commit()
    meta = {'kind': 'internal', 'name': backup.name, 'note': backup.note, 'created_at': time.time(),
            'created_by': restore.created_by, 'generation': backup.generation}
    tmp = backup_root() / '_tmp' / f'backup-{backup_id}-{uuid.uuid4().hex[:8]}'
    manifest = capture_package(tmp, meta, snapshot)
    final = package_dir(backup_id)
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        shutil.rmtree(final)
    os.replace(tmp, final)
    _finish_backup_row(backup_id, manifest, final)
    with Session() as db:
        record(db, 'system', 'backup.rollback_snapshot', {'backup': backup_id, 'restore': restore.id})
        db.commit()
    return backup_id


# ---------------------------------------------------------------- restores

def _new_restore(db, backup, actor, note, mode):
    restore = Restore(backup_id=backup.id, mode=mode, status='pending', stage='verify', progress=0,
                      message='等待执行', note=(note or '')[:500], verification={}, result={},
                      rollback_available=False, attempt=1, created_by=actor, created_at=time.time())
    db.add(restore)
    db.flush()
    return restore


def start_restore(db, backup_id, actor, note, mode='restore'):
    state = maintenance.lock_state(db)
    if state.maintenance:
        raise HTTPException(503, state.maintenance_reason or '已有恢复任务正在执行')
    if state.backup_active and mode != 'rollback':
        raise HTTPException(409, '整库备份正在进行，请备份完成后再恢复')
    backup = db.get(Backup, backup_id)
    if not backup or backup.status != 'completed':
        raise HTTPException(409, '请选择已完成的备份包')
    if not package_dir(backup.id).is_dir():
        raise HTTPException(409, '备份包文件已不存在，无法恢复')
    busy = db.scalar(select(Restore).where(Restore.status.in_(ACTIVE_RESTORE_STATUSES)).limit(1))
    if busy:
        raise HTTPException(409, '已有恢复任务正在执行，请等待或先处理中断的任务')
    # 恢复前先完整核对：坏包直接拒绝，原库与维护状态都不受影响。
    try:
        report, _, _ = verify_package(package_dir(backup.id))
    except PackageError as error:
        report = dict(error.report or {})
        report.update({'ok': False, 'message': str(error)})
        record(db, actor, 'restore.rejected', {'backup': backup_id, 'error': str(error)[:300]})
        db.commit()
        raise HTTPException(422, f'备份包核对未通过：{error}')
    restore = _new_restore(db, backup, actor, note, mode)
    restore.verification = report
    state.maintenance = True
    state.active_restore_id = restore.id
    state.maintenance_reason = f'正在从备份「{backup.name}」恢复，业务写入与分发已暂停'
    state.updated_at = time.time()
    record(db, actor, 'restore.start', {'restore': restore.id, 'backup': backup.id})
    db.commit()
    _spawn(restore.id)
    return restore


def _lock_restore(db, restore_id):
    """维护锁内以锁定读刷新恢复任务，避免用到加锁前的旧状态。"""
    return db.scalar(select(Restore).where(Restore.id == restore_id).with_for_update()
                     .execution_options(populate_existing=True))


def resume_restore(db, restore, actor):
    state = maintenance.lock_state(db)
    restore = _lock_restore(db, restore.id) or restore
    if restore.status not in ('interrupted', 'failed', 'pending'):
        raise HTTPException(409, '该恢复任务当前不可继续')
    if state.maintenance and state.active_restore_id not in (None, restore.id):
        raise HTTPException(409, '另有恢复任务正在执行')
    other = db.scalar(select(Restore).where(Restore.status.in_(ACTIVE_RESTORE_STATUSES), Restore.id != restore.id).limit(1))
    if other:
        raise HTTPException(409, '另有恢复任务正在执行')
    restore.status = 'pending'
    restore.error = ''
    restore.message = f'从检查点「{restore.stage}」继续'
    restore.attempt = int(restore.attempt or 1) + 1
    if not (restore.result or {}).get('db_switched'):
        # 尚未切换：中断期间可能有新的业务写入，旧回退快照已过期，续跑时重新生成。
        restore.rollback_backup_id = None
        restore.rollback_available = False
    state.maintenance = True
    state.active_restore_id = restore.id
    state.maintenance_reason = f'正在从备份恢复，业务写入与分发已暂停（第 {restore.attempt} 次尝试）'
    state.updated_at = time.time()
    record(db, actor, 'restore.resume', {'restore': restore.id, 'stage': restore.stage, 'attempt': restore.attempt})
    db.commit()
    _spawn(restore.id)
    return restore


def cancel_restore(db, restore, actor):
    state = maintenance.lock_state(db)
    restore = _lock_restore(db, restore.id) or restore
    if (restore.result or {}).get('db_switched'):
        raise HTTPException(409, '数据库已切换，不能取消；请使用回退操作恢复原库')
    if restore.status not in ('interrupted', 'failed', 'pending', 'running'):
        raise HTTPException(409, '该恢复任务当前无法取消')
    restore.status = 'cancelled'
    restore.message = '已取消，原库未改动'
    restore.error = ''
    restore.rollback_available = bool(restore.rollback_backup_id)
    restore.finished_at = time.time()
    state.maintenance = False
    state.active_restore_id = None
    state.maintenance_reason = ''
    state.updated_at = time.time()
    record(db, actor, 'restore.cancel', {'restore': restore.id})
    db.commit()
    return restore


def create_rollback(db, original, actor):
    state = maintenance.lock_state(db)
    original = _lock_restore(db, original.id) or original
    if not original.rollback_available or not original.rollback_backup_id:
        raise HTTPException(409, '该恢复没有可用的回退快照')
    if original.status not in ('completed', 'failed', 'interrupted'):
        raise HTTPException(409, '该恢复状态不支持回退')
    later = db.scalar(select(Restore).where(Restore.id > original.id, Restore.status == 'completed',
                                           Restore.mode == 'restore').limit(1))
    if later:
        raise HTTPException(409, '已有更新的恢复完成，该回退快照已过期')
    if state.active_restore_id not in (None, original.id):
        raise HTTPException(409, '另有恢复任务正在执行')
    backup = db.get(Backup, original.rollback_backup_id)
    if not backup or not package_dir(backup.id).is_dir():
        raise HTTPException(409, '回退快照已不存在，无法回退')
    restore = _new_restore(db, backup, actor, f'回退恢复任务 #{original.id}', mode='rollback')
    restore.result = {'rollback_of': original.id}
    state.maintenance = True
    state.active_restore_id = restore.id
    state.maintenance_reason = f'正在回退恢复任务 #{original.id}，业务写入与分发已暂停'
    state.updated_at = time.time()
    record(db, actor, 'restore.rollback', {'restore': restore.id, 'of': original.id, 'backup': backup.id})
    db.commit()
    _spawn(restore.id)
    return restore


def _spawn(restore_id):
    threading.Thread(target=_run_restore, args=(restore_id,), name=f'restore-{restore_id}', daemon=True).start()


def _update(restore_id, **fields):
    with Session() as db:
        restore = db.get(Restore, restore_id)
        if not restore:
            return None
        for key, value in fields.items():
            setattr(restore, key, value)
        db.commit()
        return restore


def _load(restore_id):
    with Session() as db:
        return db.get(Restore, restore_id)


def _run_restore(restore_id):
    with Session() as db:
        restore = db.get(Restore, restore_id)
        if not restore or restore.status == 'completed':
            return
        if restore.status == 'cancelled':
            return
        restore.status = 'running'
        restore.started_at = restore.started_at or time.time()
        db.commit()
        backup = db.get(Backup, restore.backup_id)
        backup_path = package_dir(backup.id)
        mode = restore.mode or 'restore'
        switched = bool((restore.result or {}).get('db_switched'))
    try:
        _run_stages(restore_id, backup_path, mode, switched)
    except PackageError as error:
        report = dict(error.report or {})
        report.update({'ok': False, 'message': str(error)})
        _update(restore_id, verification=report)
        _fail(restore_id, str(error))
    except Exception as error:
        logging.exception('Restore failed')
        _fail(restore_id, f'恢复执行失败：{error}')


def _fail(restore_id, message):
    with Session() as db:
        restore = db.get(Restore, restore_id)
        if not restore:
            return
        # 只依据切换事务提交后写入的标记判断，避免把“正在切换但已回滚”当成已切换。
        switched = bool((restore.result or {}).get('db_switched'))
        if restore.status != 'cancelled':
            restore.status = 'failed'
            restore.error = str(message)[:1900]
            restore.message = str(message)[:480]
            restore.finished_at = time.time()
        state = maintenance.lock_state(db)
        if not switched:
            # 尚未切换数据库：原库保持原样，释放维护锁。
            state.maintenance = False
            state.active_restore_id = None
            state.maintenance_reason = ''
            state.updated_at = time.time()
        else:
            # 已切换但未收尾：保持维护锁，等待继续或回退。
            state.maintenance = True
            state.active_restore_id = restore_id
            state.maintenance_reason = f'恢复任务 #{restore_id} 已切换数据库但未完成，请继续或回退'
            state.updated_at = time.time()
        record(db, 'system', 'restore.failed', {'restore': restore_id, 'error': str(message)[:300],
                                                'db_switched': switched})
        db.commit()


def _run_stages(restore_id, backup_path, mode, switched):
    staging = staging_dir(restore_id)
    snapshot_path = staging / 'snapshot.json'
    staged_artifacts = staging / 'artifacts'
    if not switched:
        # 每次进入（含续跑）都重新核对备份包：坏包在任何阶段都不会被切换。
        report, verified, _ = verify_package(backup_path)
        _update(restore_id, verification=report)
        restore = _load(restore_id)
        if restore.status == 'cancelled':
            return
        _update(restore_id, stage='verify', progress=STAGE_PROGRESS['verify'], message='备份包核对通过')
        _write_json(snapshot_path, verified)
        snapshot = verified
        rollback_id = restore.rollback_backup_id
        if not rollback_id:
            _update(restore_id, stage='snapshot', progress=STAGE_PROGRESS['snapshot'], message='生成恢复前回退快照')
            with Session() as db:
                live = _snapshot_tables(db)
            rollback_id = create_internal_backup(restore, '恢复前快照' if mode == 'restore' else '回退前快照', live)
            _update(restore_id, rollback_backup_id=rollback_id, rollback_available=True)
        if not staged_artifacts.is_dir():
            _update(restore_id, stage='stage', progress=STAGE_PROGRESS['stage'], message='暂存备份归档文件')
            _stage_artifacts(backup_path, staged_artifacts)
        restore = _load(restore_id)
        if restore.status == 'cancelled':
            return
        _update(restore_id, stage='switch', progress=STAGE_PROGRESS['switch'], message='切换数据库')
        if not _switch_database(restore_id, snapshot, mode):
            return
        _update(restore_id, stage='files', progress=STAGE_PROGRESS['files'], message='数据库已切换，替换归档文件',
                result={**(_load(restore_id).result or {}), 'db_switched': True})
    else:
        # 已切换数据库：以切换时暂存的快照为准，避免备份包在切换后被改动而串版本。
        if not snapshot_path.is_file():
            report, snapshot, _ = verify_package(backup_path)
            _update(restore_id, verification=report)
            _write_json(snapshot_path, snapshot)
        snapshot = _read_json(snapshot_path)
        if not staged_artifacts.is_dir():
            _stage_artifacts(backup_path, staged_artifacts)
    _replace_artifacts(staged_artifacts)
    _update(restore_id, stage='finalize', progress=STAGE_PROGRESS['finalize'], message='归档已替换，完成收尾')
    _finalize(restore_id, mode, snapshot)


def _stage_artifacts(backup_path, staged_artifacts):
    source = Path(backup_path) / 'artifacts'
    if not source.is_dir():
        raise PackageError('备份包归档目录缺失，拒绝恢复')
    if staged_artifacts.exists():
        shutil.rmtree(staged_artifacts)
    staged_artifacts.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, staged_artifacts)


def _switch_database(restore_id, snapshot, mode):
    """单事务整表替换业务数据，同时提升 generation。"""
    with Session() as db:
        state = maintenance.lock_state(db)
        restore = db.get(Restore, restore_id)
        if not restore or restore.status != 'running':
            # 已取消或已结束：不切换，原库保持原样。
            db.rollback()
            return False
        if not state.maintenance or state.active_restore_id != restore_id:
            raise RuntimeError('维护锁已失效，停止切换')
        now = time.time()
        prepared = _prepare_snapshot(snapshot, mode, now)
        # 删除与插入在同一事务：重试是整表替换，不会重复写入。
        for name in ('deliveries', 'reports', 'jobs', 'datasets', 'audits', 'users'):
            db.execute(delete(BUSINESS_TABLES[name]))
        db.flush()
        for name in ('users', 'datasets', 'jobs', 'reports', 'deliveries', 'audits'):
            rows = prepared[name]
            if rows:
                db.execute(insert(BUSINESS_TABLES[name]), rows)
        actor_email = (restore.created_by or '').lower()
        if actor_email and not any(str(row.get('email') or '').lower() == actor_email for row in prepared['users']):
            # 执行恢复的管理员账号不在该备份中：保留其登录能力，避免恢复后无人能继续操作或回退。
            current = db.scalar(select(User).where(User.email == restore.created_by))
            if current:
                used = {int(row['id']) for row in prepared['users']}
                new_id = current.id if current.id not in used else max(used | {0}) + 1
                db.execute(insert(User), [{'id': new_id, 'email': current.email, 'name': current.name,
                                           'role': current.role, 'password': current.password}])
        state.generation = int(state.generation or 1) + 1
        state.updated_at = now
        restore.result = {**(restore.result or {}), 'db_switched': True, 'switched_at': now,
                          'generation': state.generation, 'mode': mode}
        restore.progress = STAGE_PROGRESS['switch']
        restore.message = '数据库已切换'
        db.commit()
        return True


def _prepare_snapshot(snapshot, mode, now):
    prepared = {name: [dict(row) for row in snapshot.get(name, [])] for name, _, _ in TABLES}
    for user in prepared['users']:
        user['role'] = user.get('role') or 'viewer'
        user['email'] = str(user.get('email') or '')
        user['name'] = str(user.get('name') or '')
        user['password'] = str(user.get('password') or '')
    for dataset in prepared['datasets']:
        dataset['source'] = str(dataset.get('source') or 'manual.json')
        dataset['checksum'] = str(dataset.get('checksum') or '')
    for job in prepared['jobs']:
        if job.get('status') in ('queued', 'running'):
            # 未完成的分析恢复为排队，由 worker 在当前代际重新计算。
            job['status'] = 'queued'
            job['finished_at'] = None
            job['error'] = ''
        job['params'] = job.get('params') or {}
        job['result'] = job.get('result') or {}
        job['error'] = str(job.get('error') or '')
        job['approved'] = bool(job.get('approved'))
    for report in prepared['reports']:
        if mode == 'restore':
            # 恢复后计划默认停用，需负责人确认后再启用。
            report['enabled'] = False
            report['next_run'] = now + int(report.get('interval_seconds') or 86400)
        else:
            report['enabled'] = bool(report.get('enabled'))
    for delivery in prepared['deliveries']:
        delivery['created_at'] = delivery.get('created_at') or now
        delivery['checksum'] = str(delivery.get('checksum') or '')
    for audit in prepared['audits']:
        audit['detail'] = audit.get('detail') or {}
    return prepared


def _replace_artifacts(staged_artifacts):
    target = Path(ARTIFACT_DIR)
    target.mkdir(parents=True, exist_ok=True)
    for item in target.iterdir():
        if item.is_dir() and not item.is_symlink():
            shutil.rmtree(item)
        else:
            item.unlink()
    for item in sorted(staged_artifacts.iterdir()):
        if item.is_file():
            shutil.copyfile(item, target / item.name)


def _finalize(restore_id, mode, snapshot):
    expected = set(_artifact_names(snapshot))
    target = Path(ARTIFACT_DIR)
    target.mkdir(parents=True, exist_ok=True)
    for item in target.iterdir():
        if item.is_dir() and not item.is_symlink():
            shutil.rmtree(item)
        elif item.is_file() and item.name not in expected:
            item.unlink()
    _clear_queue()
    cleanup_workspace()
    with Session() as db:
        restore = db.get(Restore, restore_id)
        state = maintenance.lock_state(db)
        if not restore:
            db.rollback()
            return
        if state.active_restore_id not in (None, restore_id):
            raise RuntimeError('维护锁已失效，停止收尾')
        restore.status = 'completed'
        restore.stage = 'finalize'
        restore.progress = 100
        restore.message = '恢复完成：备份之后新增的数据不再可见，分发计划已停用'
        restore.finished_at = time.time()
        restore.result = {**(restore.result or {}), 'db_switched': True, 'mode': mode,
                          'generation': state.generation, 'completed_at': restore.finished_at,
                          'counts': {name: len(rows) for name, rows in snapshot.items()},
                          'artifacts': len(expected)}
        state.maintenance = False
        state.active_restore_id = None
        state.maintenance_reason = ''
        state.updated_at = time.time()
        rollback_of = (restore.result or {}).get('rollback_of')
        if mode == 'rollback' and rollback_of:
            original = db.get(Restore, rollback_of)
            if original:
                original.status = 'rolled_back'
                original.message = f'已按回退任务 #{restore_id} 恢复原库'
        record(db, restore.created_by,
               'restore.rolled_back' if mode == 'rollback' else 'restore.completed',
               {'restore': restore_id, 'generation': state.generation, 'backup': restore.backup_id})
        db.commit()
    shutil.rmtree(staging_dir(restore_id), ignore_errors=True)


def cleanup_workspace(keep_restores=10):
    """清理临时文件与过期回退快照包；记录保留用于审计，但文件不再可供回退。"""
    root = backup_root()
    shutil.rmtree(root / '_tmp', ignore_errors=True)
    with Session() as db:
        restores = list(db.scalars(select(Restore).order_by(Restore.id.desc())))
        recent = {item.id for item in restores[:keep_restores]}
        # 未完成、失败或中断的恢复仍可能续跑或回退，其暂存与回退包必须保留。
        keep_restores_all = {item.id for item in restores
                             if item.status not in ('completed', 'cancelled', 'rolled_back')}
        keep_staging = recent | keep_restores_all
        keep_packages = {item.rollback_backup_id for item in restores if item.id in keep_staging}
        keep_packages.discard(None)
        stale = [item for item in db.scalars(select(Backup).where(Backup.internal == True, Backup.status == 'completed'))  # noqa: E712
                 if item.id not in keep_packages]
    for item in stale:
        shutil.rmtree(package_dir(item.id), ignore_errors=True)
        with Session() as db:
            row = db.get(Backup, item.id)
            if row and row.status == 'completed':
                row.status = 'pruned'
                row.note = (row.note or '')[:460] + '（已按保留策略清理，仅保留审计记录）'
            for restore in db.scalars(select(Restore).where(Restore.rollback_backup_id == item.id)):
                restore.rollback_available = False
            db.commit()
    staging = root / '_staging'
    if staging.is_dir():
        for item in staging.iterdir():
            try:
                restore_id = int(item.name.rsplit('-', 1)[-1])
            except ValueError:
                continue
            if restore_id not in keep_staging:
                shutil.rmtree(item, ignore_errors=True)


def _clear_queue():
    try:
        from redis import Redis
        from src.config.settings import REDIS_URL
        client = Redis.from_url(REDIS_URL, socket_timeout=2)
        client.delete('analysis:queue')
        client.close()
    except Exception:
        logging.warning('Redis queue cleanup skipped')


def recover_interrupted():
    """进程重启后收拾未完成的备份/恢复；已切换数据库的恢复保持锁定等待继续。"""
    with Session() as db:
        state = maintenance.ensure_state(db)
        for backup in db.scalars(select(Backup).where(Backup.status == 'creating')):
            backup.status = 'failed'
            backup.error = '服务重启，备份未完成'
            backup.finished_at = time.time()
        state.maintenance = False
        state.active_restore_id = None
        state.maintenance_reason = ''
        for restore in db.scalars(select(Restore).where(Restore.status.in_(ACTIVE_RESTORE_STATUSES))):
            restore.status = 'interrupted'
            restore.finished_at = None
            if (restore.result or {}).get('db_switched'):
                restore.message = '服务中断：数据库已切换，请继续收尾或回退'
                state.maintenance = True
                state.active_restore_id = restore.id
                state.maintenance_reason = f'恢复任务 #{restore.id} 已切换数据库但未完成，请继续或回退'
            else:
                restore.message = '服务中断：原库未改动，可从检查点继续'
        state.backup_active = False
        state.updated_at = time.time()
        db.commit()


# ---------------------------------------------------------------- serialization

def backup_dict(backup):
    return {'id': backup.id, 'name': backup.name, 'note': backup.note, 'status': backup.status,
            'created_by': backup.created_by, 'created_at': backup.created_at, 'finished_at': backup.finished_at,
            'generation': backup.generation, 'row_counts': backup.row_counts or {}, 'row_total': backup.row_total,
            'artifact_count': backup.artifact_count, 'size_bytes': backup.size_bytes,
            'package_sha256': backup.package_sha256, 'error': backup.error, 'internal': bool(backup.internal),
            'verification': backup.verification or {}}


def restore_dict(restore):
    return {'id': restore.id, 'backup_id': restore.backup_id, 'status': restore.status, 'stage': restore.stage,
            'progress': restore.progress, 'message': restore.message, 'error': restore.error, 'note': restore.note,
            'verification': restore.verification or {}, 'result': restore.result or {},
            'rollback_available': bool(restore.rollback_available),
            'rollback_backup_id': restore.rollback_backup_id, 'attempt': restore.attempt,
            'mode': restore.mode, 'created_by': restore.created_by, 'created_at': restore.created_at,
            'started_at': restore.started_at, 'finished_at': restore.finished_at}
