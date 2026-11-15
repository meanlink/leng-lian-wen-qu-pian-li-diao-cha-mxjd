"""备份/恢复行为测试：并发、坏包、中断续跑、晚到计算、回退。"""
import json
import pathlib
import sys
import time

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2] / 'backend'))

from fastapi.testclient import TestClient  # noqa: E402
from src import worker  # noqa: E402
from src.config.database import Session  # noqa: E402
from src.main import app  # noqa: E402
from src.services import maintenance, ops  # noqa: E402
from src.services.business import seed_records  # noqa: E402
from scripts.seed_demo import seed  # noqa: E402


@pytest.fixture(scope='module')
def client():
    with TestClient(app) as c:
        seed()
        yield c


@pytest.fixture(scope='module')
def admin(client):
    token = client.post('/auth/login', json={'email': 'admin@demo.local', 'password': 'Demo123!'}).json()['access_token']
    return {'Authorization': 'Bearer ' + token}


def wait_restore(client, headers, restore_id, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f'/restores/{restore_id}', headers=headers).json()
        if data['status'] in ('completed', 'failed', 'interrupted', 'cancelled', 'rolled_back'):
            return data
        time.sleep(0.05)
    raise AssertionError('restore timeout ' + json.dumps(data, ensure_ascii=False))


def wait_backup(client, headers, backup_id, timeout=40):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = client.get(f'/backups/{backup_id}', headers=headers).json()
        if data['status'] in ('completed', 'failed'):
            return data
        time.sleep(0.05)
    raise AssertionError('backup timeout')


def make_backup(client, admin, name='测试备份'):
    response = client.post('/backups', json={'name': name, 'note': '自动化测试'}, headers=admin)
    assert response.status_code == 201, response.text
    backup = wait_backup(client, admin, response.json()['id'])
    assert backup['status'] == 'completed', backup
    return backup


def add_dataset(client, admin, name, records=None):
    if records is None:
        records = seed_records()
    response = client.post('/datasets', json={'name': name, 'source': 'test.json', 'records': records}, headers=admin)
    assert response.status_code == 201, response.text
    return response.json()


def dataset_names(client, admin):
    return [item['name'] for item in client.get('/datasets', headers=admin).json()]


def test_backup_contains_rows_and_artifacts(client, admin):
    backup = make_backup(client, admin, '结构备份')
    assert backup['row_counts']['datasets'] >= 1
    assert backup['artifact_count'] >= 1
    report = client.post(f"/backups/{backup['id']}/verify", headers=admin)
    assert report.status_code == 200
    body = report.json()
    assert body['ok'] is True
    assert body['tables']['jobs']['ok'] is True
    assert body['artifacts']['verified'] == body['artifacts']['count']
    assert body['links']['checked'] >= 2


def test_restore_hides_new_data_and_disables_schedules(client, admin):
    backup = make_backup(client, admin, '恢复点 A')
    add_dataset(client, admin, '备份后批次 A')
    jobs_before = client.get('/jobs', headers=admin).json()
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '误操作回滚'}, headers=admin)
    assert response.status_code == 201, response.text
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed', restore
    assert restore['progress'] == 100
    assert restore['result']['db_switched'] is True
    assert '备份后批次 A' not in dataset_names(client, admin)
    # 备份之后新增的批次与分析都不可见，恢复点内的任务保持原样。
    assert len(client.get('/jobs', headers=admin).json()) == backup['row_counts']['jobs'] == len(jobs_before)
    reports = client.get('/reports', headers=admin).json()
    assert all(item['enabled'] is False for item in reports)
    deliveries = client.get('/deliveries', headers=admin).json()
    assert deliveries
    for item in deliveries:
        assert client.get(f"/deliveries/{item['id']}/download?format=csv", headers=admin).status_code == 200
        assert client.get(f"/deliveries/{item['id']}/download?format=json", headers=admin).status_code == 200
    status = client.get('/ops/status', headers=admin).json()
    assert status['maintenance'] is False
    assert status['generation'] == restore['result']['generation']


def test_writes_blocked_during_maintenance(client, admin):
    with Session() as db:
        state = maintenance.ensure_state(db)
        state.maintenance = True
        state.maintenance_reason = '测试维护'
        db.commit()
    try:
        response = client.post('/datasets', json={'name': '维护中批次', 'records': seed_records()}, headers=admin)
        assert response.status_code == 503
        assert '维护' in response.json()['detail'] or '恢复' in response.json()['detail']
        response = client.post('/jobs', json={'name': '维护中任务', 'dataset_id': 1, 'params': {}}, headers=admin)
        assert response.status_code == 503
        assert client.get('/datasets', headers=admin).status_code == 200
    finally:
        with Session() as db:
            state = maintenance.ensure_state(db)
            state.maintenance = False
            state.maintenance_reason = ''
            db.commit()


def test_queued_job_survives_and_is_recomputed(client, admin):
    dataset = add_dataset(client, admin, '排队恢复批次')
    job = client.post('/jobs', json={'name': '排队调查', 'dataset_id': dataset['id'], 'params': {'upper': 8, 'alpha': '0.05'}}, headers=admin).json()
    backup = make_backup(client, admin, '排队恢复点')
    client.post(f"/jobs/{job['id']}/approve", headers=admin)
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '保留排队任务'}, headers=admin)
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed', restore
    # 备份时任务仍在排队，恢复后应保留为排队并可继续执行。
    job_after = client.get(f"/jobs/{job['id']}", headers=admin).json()
    assert job_after['status'] == 'queued'
    worker.execute(job['id'])
    job_after = client.get(f"/jobs/{job['id']}", headers=admin).json()
    assert job_after['status'] == 'completed', job_after
    assert job_after['result']['summary']


def test_bad_package_rejected_and_library_kept(client, admin, tmp_path):
    backup = make_backup(client, admin, '坏包测试')
    before = dataset_names(client, admin)
    package = ops.package_dir(backup['id'])
    data_file = package / 'data' / 'jobs.json'
    original = data_file.read_bytes()
    data_file.write_bytes(original.replace(b'"status"', b'"status" ', 1))
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '坏包'}, headers=admin)
    assert response.status_code == 422
    assert '指纹' in response.json()['detail'] or '拒绝' in response.json()['detail']
    assert dataset_names(client, admin) == before
    status = client.get('/ops/status', headers=admin).json()
    assert status['maintenance'] is False
    assert all(item['backup_id'] != backup['id'] for item in client.get('/restores', headers=admin).json())
    data_file.write_bytes(original)
    # 修复后重试同一备份可成功，且不会重复写入。
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '修复后重试'}, headers=admin)
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed', restore
    datasets = client.get('/datasets', headers=admin).json()
    assert len(datasets) == len({item['id'] for item in datasets})
    assert len(datasets) == backup['row_counts']['datasets']


def test_interrupted_restore_can_resume(client, admin):
    backup = make_backup(client, admin, '中断恢复点')
    add_dataset(client, admin, '中断后新增批次')
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '模拟中断'}, headers=admin)
    restore_id = response.json()['id']
    wait_restore(client, admin, restore_id)
    # 模拟进程中断：把任务标为 interrupted，维护锁保持。
    with Session() as db:
        from src.models import Restore
        restore = db.get(Restore, restore_id)
        restore.status = 'interrupted'
        restore.finished_at = None
        state = maintenance.ensure_state(db)
        state.maintenance = True
        state.active_restore_id = restore_id
        state.maintenance_reason = '模拟中断'
        db.commit()
    response = client.post(f'/restores/{restore_id}/resume', headers=admin)
    assert response.status_code == 200, response.text
    restored = wait_restore(client, admin, restore_id)
    assert restored['status'] == 'completed', restored
    assert restored['attempt'] == 2
    names = dataset_names(client, admin)
    assert '中断后新增批次' not in names
    assert len(names) == backup['row_counts']['datasets']


def test_rollback_restores_original(client, admin):
    backup = make_backup(client, admin, '回退测试点')
    add_dataset(client, admin, '回退前新增批次')
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '准备回退'}, headers=admin)
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed'
    assert '回退前新增批次' not in dataset_names(client, admin)
    rollback = client.post(f"/restores/{restore['id']}/rollback", headers=admin)
    assert rollback.status_code == 201, rollback.text
    rolled = wait_restore(client, admin, rollback.json()['id'])
    assert rolled['status'] == 'completed', rolled
    assert rolled['mode'] == 'rollback'
    assert '回退前新增批次' in dataset_names(client, admin)
    original = client.get(f"/restores/{restore['id']}", headers=admin).json()
    assert original['status'] == 'rolled_back'


def test_late_worker_result_discarded(client, admin, monkeypatch):
    backup = make_backup(client, admin, '晚到结果测试')
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '晚到计算'}, headers=admin)
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed'
    dataset = client.get('/datasets', headers=admin).json()[0]
    job = client.post('/jobs', json={'name': '晚到调查', 'dataset_id': dataset['id'], 'params': {'upper': 8, 'alpha': '0.05'}}, headers=admin).json()
    generation = worker.claim_job(job['id'])
    assert generation is not None
    original_result = {'summary': 'stale', 'metrics': [], 'table': {'columns': [], 'rows': []}}
    # 模拟恢复切换：代际提升且维护锁关闭后，晚到的结果不得写入。
    with Session() as db:
        state = maintenance.ensure_state(db)
        state.generation += 1
        db.commit()
    worker._finish_completed(job['id'], generation, original_result)
    job_after = client.get(f"/jobs/{job['id']}", headers=admin).json()
    assert job_after['status'] == 'running'
    assert job_after['result'] != original_result


def test_non_admin_cannot_backup_or_restore(client, admin):
    token = client.post('/auth/login', json={'email': 'analyst@demo.local', 'password': 'Demo123!'}).json()['access_token']
    headers = {'Authorization': 'Bearer ' + token}
    assert client.post('/backups', json={'name': '分析员备份'}, headers=headers).status_code == 403
    assert client.post('/restores', json={'backup_id': 1}, headers=headers).status_code == 403
    assert client.get('/backups', headers=headers).status_code == 200


def test_backup_is_consistent_with_concurrent_delivery(client, admin, monkeypatch):
    """备份快照与运行中的分发并发：包内归档必须与数据快照对得上。"""
    dataset = add_dataset(client, admin, '并发备份批次')
    job = client.post('/jobs', json={'name': '并发分发调查', 'dataset_id': dataset['id'],
                                     'params': {'upper': 8, 'alpha': '0.05'}}, headers=admin).json()
    worker.execute(job['id'])
    assert client.get(f"/jobs/{job['id']}", headers=admin).json()['status'] == 'completed'
    client.post(f"/jobs/{job['id']}/approve", headers=admin)
    client.post('/reports', json={'name': '并发分发计划', 'job_id': job['id'],
                                  'destination': '并发组', 'interval_seconds': 60,
                                  'delay_seconds': 0}, headers=admin)
    worker.deliver_due()
    original_deliver = worker.deliver

    def delivering_during_backup(report_id):
        # 备份进行中触发一次新的分发，验证不会串版本。
        worker.deliver_one(report_id)
        return original_deliver(report_id)

    monkeypatch.setattr(worker, 'deliver', delivering_during_backup)
    response = client.post('/backups', json={'name': '并发一致性备份'}, headers=admin)
    backup = wait_backup(client, admin, response.json()['id'])
    assert backup['status'] == 'completed'
    report = client.post(f"/backups/{backup['id']}/verify", headers=admin)
    assert report.status_code == 200, report.text
    body = report.json()
    assert body['ok'] is True
    assert body['artifacts']['verified'] == body['artifacts']['count']
    assert body['tables']['deliveries']['count'] >= 1


def test_restore_rejects_missing_artifact(client, admin):
    backup = make_backup(client, admin, '缺文件备份')
    before = dataset_names(client, admin)
    package = ops.package_dir(backup['id'])
    victim = sorted((package / 'artifacts').iterdir())[0]
    victim.unlink()
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '缺归档'}, headers=admin)
    assert response.status_code == 422
    assert '归档' in response.json()['detail']
    assert dataset_names(client, admin) == before
    assert client.get('/ops/status', headers=admin).json()['maintenance'] is False


def test_cancel_before_switch_keeps_library(client, admin):
    backup = make_backup(client, admin, '取消测试点')
    add_dataset(client, admin, '取消前新增批次')
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '取消恢复'}, headers=admin)
    restore_id = response.json()['id']
    wait_restore(client, admin, restore_id)
    # 已完成（已切换）的恢复只能回退，不能取消。
    assert client.post(f'/restores/{restore_id}/cancel', headers=admin).status_code == 409
    # 模拟切换前中断：取消后维护锁释放，原库保持原样。
    from src.models import Restore
    backup2 = make_backup(client, admin, '取消测试点 2')
    add_dataset(client, admin, '取消前新增批次 2')
    # 直接构造“核对/暂存阶段中断、尚未切换数据库”的恢复任务，避免后台线程抢先切换。
    with Session() as db:
        restore = Restore(backup_id=backup2['id'], mode='restore', status='interrupted', stage='stage',
                          progress=40, message='服务中断：原库未改动，可从检查点继续', error='', note='',
                          verification={}, result={}, rollback_available=True, attempt=1,
                          created_by='admin@demo.local', created_at=time.time())
        db.add(restore)
        db.flush()
        pending_id = restore.id
        state = maintenance.ensure_state(db)
        state.maintenance = True
        state.active_restore_id = pending_id
        state.maintenance_reason = '模拟切换前中断'
        db.commit()
    cancelled = client.post(f'/restores/{pending_id}/cancel', headers=admin)
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()['status'] == 'cancelled'
    assert '取消前新增批次 2' in dataset_names(client, admin)
    assert client.get('/ops/status', headers=admin).json()['maintenance'] is False


def test_delivery_blocked_and_jobs_kept_after_restore(client, admin):
    backup = make_backup(client, admin, '分发阻断备份')
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '阻断分发'}, headers=admin)
    restore = wait_restore(client, admin, response.json()['id'])
    assert restore['status'] == 'completed'
    # 恢复后计划停用：worker 不会自动再分发。
    before = len(client.get('/deliveries', headers=admin).json())
    worker.deliver_due()
    assert len(client.get('/deliveries', headers=admin).json()) == before


def test_malformed_package_returns_422_not_500(client, admin):
    backup = make_backup(client, admin, '畸形包测试')
    package = ops.package_dir(backup['id'])
    manifest = json.loads((package / 'manifest.json').read_text())
    manifest['tables']['jobs']['count'] = 'not-a-number'
    (package / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False))
    response = client.post('/restores', json={'backup_id': backup['id'], 'note': '畸形'}, headers=admin)
    assert response.status_code == 422, response.text
    assert client.get('/ops/status', headers=admin).json()['maintenance'] is False
