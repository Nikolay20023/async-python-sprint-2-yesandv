import pytest

from job import Job
from models import State


@pytest.fixture
def job():
    return Job(func=sum, args=([1, 10, 100],))


def test_init_job_run(job):
    job.run()
    assert job.state == State.RUNNING


def test_run_job(job):
    coro = job.run()
    next(coro)
    assert job.state == State.COMPLETED


def test_fail_job():
    def throw_ex():
        raise TypeError("One of the variables is not an integer")

    job = Job(func=throw_ex)
    with pytest.raises(TypeError):
        coro = job.run()
        next(coro)
    assert job.state == State.FAILED


def test_job_retries():
    def throw_ex():
        if throw_ex.counter > 0:
            throw_ex.counter -= 1
            raise TypeError("One of the variables is not an integer")
        else:
            print("🍀")

    throw_ex.counter = 2

    job = Job(func=throw_ex, max_retries=2)
    coro = job.run()
    next(coro)
    assert job.state == State.COMPLETED
