import os

import pytest
from scheduler import Scheduler, Job


@pytest.fixture
def scheduler():
    return Scheduler()


@pytest.fixture
def job():
    return Job(func=sum, args=([1, 10, 100],))


def test_schedule_job(scheduler, job):
    scheduler.schedule(jobs=[job])
    assert len(scheduler._pending_jobs) == 1


def test_save_jobs(scheduler, job):
    scheduler.schedule(jobs=[job])
    scheduler._save_jobs()
    assert os.path.isfile(scheduler._lock_file)


def test_load_jobs(scheduler, job):
    scheduler._running_jobs.append(job)
    scheduler._save_jobs()
    scheduler._load_jobs()
    assert job in scheduler._running_jobs
