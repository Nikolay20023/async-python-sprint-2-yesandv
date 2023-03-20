import time

from job import Job
from pipeline import pipeline
from scheduler import Scheduler
from tasks import RequestTask, FileTask, DirectoryTask


def main():
    rt = RequestTask()
    job_1 = Job(
        func=rt.call,
        args=("GET", "https://httpbin.org/deny"),
        start_at=time.monotonic() + 3,
    )
    job_2 = Job(func=rt.parse_response, dependencies=[job_1])
    dt = DirectoryTask(path="new_dir")
    job_3 = Job(func=dt.create)
    ft = FileTask(file_name="new_dir/new_file.txt")
    job_4 = Job(func=ft.create, dependencies=[job_3])
    job_5 = Job(
        func=ft.write,
        args=(open("lorem_ipsum.txt").read(),),
        dependencies=[job_4],
    )
    job_6 = Job(func=ft.delete, dependencies=[job_5])
    job_7 = Job(func=dt.delete, dependencies=[job_6])
    job_8 = Job(func=pipeline)
    scheduler = Scheduler()
    jobs = [job_1, job_2, job_3, job_4, job_5, job_6, job_7, job_8]
    jobs.reverse()
    scheduler.schedule(jobs=jobs)
    scheduler.run()


if __name__ == "__main__":
    main()
