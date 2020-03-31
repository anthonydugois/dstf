import abc
import collections
from typing import Iterator, List, Dict


class Error(Exception):
    pass


class ConstraintError(Error):
    pass


class Constraint(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        pass


class Operator(metaclass=abc.ABCMeta):
    def __init__(self, task: "Task", start_time: float, proc_times: Dict["Node", float]):
        self.task = task
        self.start_time = start_time
        self.proc_times = proc_times

    @abc.abstractmethod
    def apply(self, schedule: "Schedule") -> "Schedule":
        pass


class Node:
    def __init__(self, name: str):
        self.name = name


class Task:
    def __init__(self, name: str):
        self.name = name

        self.constraints = collections.OrderedDict()

    def __contains__(self, constraint_cls):
        return constraint_cls in self.constraints

    def __getitem__(self, constraint_cls):
        return self.constraints[constraint_cls]

    def __getattr__(self, attr):
        for ctr in self.constraints.values():
            if attr in ctr.__dict__:
                return ctr.__dict__[attr]

        raise AttributeError("'{}' task has no attribute '{}'".format(self.name, attr))

    def set(self, constraint):
        self.constraints[type(constraint)] = constraint


class Chunk:
    def __init__(self, task: "Task", start_time: float, proc_times: Dict["Node", float]):
        self.task = task
        self.start_time = start_time
        self.proc_times = proc_times

    def is_valid(self, schedule: "Schedule") -> bool:
        for ctr in self.task.constraints.values():
            if not ctr.is_valid(schedule, self):
                return False

        return True


class Schedule:
    def __init__(self):
        self.chunk_map = {}

    def __getitem__(self, task: "Task") -> List["Chunk"]:
        return self.chunk_map[task]

    def __contains__(self, task: "Task") -> bool:
        return task in self.chunk_map

    def __iter__(self) -> Iterator["Task"]:
        return iter(self.chunk_map)

    def __len__(self) -> int:
        return len(self.chunk_map)

    def update(self, operator: "Operator") -> "Schedule":
        return operator.apply(self)

    def append(self, task: "Task", start_time: float, proc_times: Dict["Node", float]):
        chunk = Chunk(task, start_time, proc_times)

        if not chunk.is_valid(self):
            raise ConstraintError

        if chunk.task in self.chunk_map:
            self.chunk_map[chunk.task].append(chunk)
        else:
            self.chunk_map[chunk.task] = [chunk]
