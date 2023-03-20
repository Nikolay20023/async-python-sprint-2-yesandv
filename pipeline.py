import logging

from tasks import FileTask, RequestTask

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_to_file():
    try:
        while True:
            filename, content = yield
            FileTask(file_name=filename).write(content)
    except GeneratorExit:
        logger.info("Final stage")


def parse_response():
    output = save_to_file()
    next(output)
    try:
        while True:
            response = yield
            logger.info("Parsing the response")
            rt = RequestTask(response=response)
            parsed_response = rt.parse_response()
            output.send(("response.txt", parsed_response))
    except GeneratorExit:
        output.close()


def call_request(method: str, url: str):
    logger.info("Starting the pipeline")
    output = parse_response()
    next(output)
    rt = RequestTask()
    rt.call(method, url)
    logger.info("Sent through")
    output.send(rt.response)
    output.close()


def pipeline():
    call_request("GET", "https://httpbin.org/deny")
