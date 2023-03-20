import logging
import os
from json import JSONDecodeError

import requests
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DirectoryTask(BaseModel):
    path: str | None

    def create(self):
        try:
            os.mkdir(self.path)
        except FileExistsError as ex:
            logger.exception("Directory '%s' already exists", self.path)
            raise ex

    def delete(self):
        try:
            os.rmdir(self.path)
        except NotADirectoryError as ex:
            logger.exception(
                "%s can't be removed since it's not a directory", self.path
            )
            raise ex
        except OSError as ex:
            logger.exception("%s is not empty", self.path)
            raise ex

    def rename(self, new_name: str):
        os.rename(self.path, new_name)


class FileTask(BaseModel):
    file_name: str | None

    def create(self):
        try:
            return open(self.file_name, "x")
        except FileExistsError as ex:
            logger.exception("%s already exists", self.file_name)
            raise ex

    def read(self):
        with open(self.file_name) as file:
            file.read()

    def write(self, new_content: str):
        try:
            with open(self.file_name, "w") as file:
                file.write(new_content)
                file.close()
        except FileNotFoundError as ex:
            raise ex

    def delete(self):
        os.remove(self.file_name)


class RequestTask(BaseModel):
    response: requests.Response | None
    _session = requests.session()

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def call(self, method: str, url: str, **kwargs):
        logger.info("Sending a %s request to %s", method.upper(), url)
        self.response = self._session.request(method.upper(), url, **kwargs)
        self.response.raise_for_status()

    def parse_response(self) -> dict | str:
        try:
            return self.response.json()
        except JSONDecodeError:
            logger.info("Failed to decode JSON data")
            logger.info("Returning a str: %s", self.response.text)
            return self.response.text
