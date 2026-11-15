"""测试统一使用独立的临时数据库与目录，避免污染本地数据。"""
import os
import pathlib
import shutil
import tempfile

ROOT = pathlib.Path(tempfile.mkdtemp(prefix='ce-tests-'))
os.environ.setdefault('DATABASE_URL', 'sqlite:///' + str(ROOT / 'test.db'))
os.environ.setdefault('ARTIFACT_DIR', str(ROOT / 'artifacts'))
os.environ.setdefault('BACKUP_DIR', str(ROOT / 'backups'))
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:6399/0')
os.environ.setdefault('SEED_DEMO', 'false')
os.environ.setdefault('JWT_SECRET', 'test-secret')


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(ROOT, ignore_errors=True)
