import time, json, logging
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report
from src.services import maintenance
from src.services.business import analyze
from src.services.reports import deliver
from src.services.audit import record
logging.basicConfig(level=logging.INFO)

# 本进程正在执行的任务。单 worker 部署下，running 但不在集合中的任务视为遗留。
ACTIVE = set()


def claim_job(job_id):
    """维护锁内认领任务，返回认领时的数据代际；不可执行时返回 None。

    恢复切换持有同一把锁，因此“认领”与“切换”不会交错。
    """
    with Session() as db:
        if not maintenance.claim_write_slot(db):
            db.rollback()
            return None
        j = db.get(Job, job_id)
        if not j or j.status != 'queued':
            db.rollback()
            return None
        generation = maintenance.read_generation(db)
        j.status = 'running'
        db.commit()
        return generation


def execute(job_id):
    generation = claim_job(job_id)
    if generation is None:
        return
    ACTIVE.add(job_id)
    try:
        _execute_claimed(job_id, generation)
    finally:
        ACTIVE.discard(job_id)


def _execute_claimed(job_id, generation):
    with Session() as db:
        j = db.get(Job, job_id)
        if not j:
            return
        dataset_id, params = j.dataset_id, dict(j.params or {})
        d = db.get(Dataset, dataset_id)
        records = d.records if d else None
        db.rollback()
    try:
        if records is None:
            raise ValueError('数据批次在恢复切换后已不可见，任务无法执行')
        result = analyze(records, params)
        json.dumps(result, allow_nan=False)
    except Exception as e:
        logging.exception('Analysis failed')
        _finish_failed(job_id, generation, str(e))
        return
    _finish_completed(job_id, generation, result)


def _finish_completed(job_id, generation, result):
    with Session() as db:
        # 第二次核对：计算期间若发生恢复切换，晚到的结果必须丢弃。
        if not maintenance.claim_write_slot(db) or not maintenance.generation_matches(db, generation):
            db.rollback()
            with Session() as log:
                record(log, 'worker', 'job.stale_result', {'id': job_id, 'generation': generation})
                log.commit()
            logging.info('Discarded late result for job %s (generation %s)', job_id, generation)
            return
        j = db.get(Job, job_id)
        if not j or j.status != 'running':
            db.rollback()
            return
        j.result = result
        j.status = 'completed'
        j.finished_at = time.time()
        record(db, 'worker', 'job.completed', {'id': job_id, 'generation': generation})
        db.commit()


def _finish_failed(job_id, generation, error):
    with Session() as db:
        if not maintenance.claim_write_slot(db) or not maintenance.generation_matches(db, generation):
            db.rollback()
            with Session() as log:
                record(log, 'worker', 'job.stale_failure', {'id': job_id, 'generation': generation})
                log.commit()
            return
        j = db.get(Job, job_id)
        if not j or j.status != 'running':
            db.rollback()
            return
        j.status = 'failed'
        j.error = error[:990]
        j.finished_at = time.time()
        record(db, 'worker', 'job.failed', {'id': job_id, 'error': j.error})
        db.commit()


def queued_jobs():
    with Session() as db:
        rows = list(db.scalars(select(Job.id).where(Job.status == 'queued').limit(3)))
        db.rollback()
        return rows


def deliver_due():
    with Session() as db:
        due = list(db.scalars(select(Report.id).where(Report.enabled == True, Report.next_run <= time.time())))  # noqa: E712
        db.rollback()
    for report_id in due:
        deliver_one(report_id)


def deliver_one(report_id):
    with Session() as db:
        # 分发与恢复切换使用同一把维护锁，不会跨代际写出归档。
        if not maintenance.claim_write_slot(db):
            db.rollback()
            return
        generation = maintenance.read_generation(db)
        report = db.get(Report, report_id)
        if not report or not report.enabled or report.next_run > time.time():
            db.rollback()
            return
        if not maintenance.generation_matches(db, generation):
            db.rollback()
            return
        d = deliver(db, report)
        if d:
            record(db, 'worker', 'report.delivered', {'id': report_id, 'delivery': d.id, 'generation': generation})
        db.commit()


def tick():
    with Session() as db:
        blocked = maintenance.maintenance_active(db)
        db.rollback()
    if blocked:
        return
    for job_id in queued_jobs():
        try:
            execute(job_id)
        except Exception:
            logging.exception('Worker job retry')
    deliver_due()


def main():
    r = Redis.from_url(REDIS_URL, socket_timeout=3)
    # This Compose stack has a single worker. A crash must not strand its jobs.
    with Session() as db:
        if maintenance.claim_write_slot(db):
            for j in db.scalars(select(Job).where(Job.status == 'running')):
                if j.id in ACTIVE:
                    continue
                j.status = 'queued'
                record(db, 'worker', 'job.recovered', {'id': j.id})
            db.commit()
        else:
            # 恢复未收尾：任务保持 running，待恢复完成后的重试再处理。
            db.rollback()
    while True:
        Path('/tmp/heartbeat').touch()
        try:
            item = r.blpop('analysis:queue', timeout=1)
            if item:
                execute(int(item[1]))
            tick()
        except Exception:
            logging.exception('Worker retry')
            time.sleep(2)


if __name__ == '__main__':
    main()
