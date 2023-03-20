import logging
import pickle
from collections import deque

from pydantic import BaseModel

from job import Job
from models import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Scheduler(BaseModel):
    pool_size: int = 10
    _pending_jobs: deque[Job] = deque()
    _running_jobs: deque[Job] = deque()
    _lock_file: str = "scheduler.txt"

    class Config:
        underscore_attrs_are_private = True

    def schedule(self, jobs: list[Job]):
        self._pending_jobs.extend(jobs)

    def _run_coroutine(self, job: Job):
        self._running_jobs.append(job)
        coro = job.run()
        try:
            while True:
                next(coro)
        except (StopIteration, GeneratorExit):
            logger.info("Gen execution has completed")
            self._running_jobs.remove(job)

    def run(self):
        logger.info("The scheduler is up and running")
        while self._pending_jobs and len(self._running_jobs) < self.pool_size:
            job = self._pending_jobs.popleft()
            if job.dependencies and not all(
                dep.state == State.COMPLETED for dep in job.dependencies
            ):
                logger.info(
                    "%s dependencies are yet to be completed",
                    job.func.__name__,
                )
                self._pending_jobs.append(job)
            else:
                self._run_coroutine(job)

    def _load_jobs(self):
        with open(self._lock_file, "rb") as f:
            self._running_jobs, self._pending_jobs = pickle.load(f)

    def restart(self):
        logger.info("Restarting the scheduler")
        self._load_jobs()
        self.run()

    def _save_jobs(self):
        with open(self._lock_file, "wb") as f:
            pickle.dump((self._running_jobs, self._pending_jobs), f)

    def stop(self):
        logger.info("Stopping the scheduler")
        [job.stop() for job in self._running_jobs + self._pending_jobs]
        self._save_jobs()
