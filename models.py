from enum import Enum


class State(Enum):
    NOT_STARTED = "Not started"
    RUNNING = "Running"
    PAUSED = "Paused"
    STOPPED = "Stopped"
    COMPLETED = "Completed"
    FAILED = "Failed"
