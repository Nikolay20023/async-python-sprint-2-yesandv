import logging
import time
from typing import Callable

from pydantic import BaseModel

from models import State

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def coroutine(func: Callable) -> Callable:
    def wrap(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen

    return wrap


class Job(BaseModel):
    func: Callable
    args: tuple | None = ()
    kwargs: dict | None = {}
    _start_time: float | None
    start_at: float | None
    timeout: float | None
    max_retries: int = 0
    dependencies: list | None
    state: State = State.NOT_STARTED

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def _retry(self):
        while self.max_retries > 0:
            logger.info("Retrying")
            self.max_retries -= 1
            yield from self.run()
        logger.exception("Exceeded max retries")

    def _delay(self):
        self.state = State.PAUSED
        if (delta := self.start_at - self._start_time) > 0:
            logger.info("%d sec. delay", delta)
            time.sleep(delta)
            self._start_time = time.monotonic()

    @coroutine
    def run(self):
        self._start_time = time.monotonic()
        logger.info("Executing the job '%s'", self.func.__name__)
        if self.start_at:
            self._delay()
        self.state = State.RUNNING
        try:
            _ = (yield)
            self.func(*self.args, **self.kwargs)
            self.state = State.COMPLETED
            logger.info("Back to the scheduler")
            yield
        except (TypeError, AttributeError) as ex:
            if self.max_retries:
                yield from self._retry()
            self.state = State.FAILED
            logger.exception("Job has failed")
            raise ex

    def stop(self):
        self.state = State.STOPPED
