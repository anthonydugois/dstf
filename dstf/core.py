from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import Iterator, Any, List, Dict, Type, Optional


class Error(Exception):
    pass


class ConstraintError(Error):
    pass


class Constraint(metaclass=ABCMeta):
    @abstractmethod
    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        pass

    def get_error(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' constraint is not met".format(type(self).__name__)


class Property(metaclass=ABCMeta):
    @abstractmethod
    def get(self, schedule: "Schedule") -> Any:
        pass


class Operator(metaclass=ABCMeta):
    def __init__(self, task: "Task", start_time: float, proc_times: Dict[Any, float]):
        self.task = task
        self.start_time = start_time
        self.proc_times = proc_times

    @abstractmethod
    def apply(self, schedule: "Schedule"):
        pass


class Task:
    def __init__(self, name: str):
        self.name = name

        self.constraints = OrderedDict()

    def __contains__(self, constraint_cls: Type["Constraint"]) -> bool:
        return constraint_cls in self.constraints

    def __iter__(self) -> Iterator[Type["Constraint"]]:
        return iter(self.constraints)

    def __getitem__(self, constraint_cls: Type["Constraint"]) -> "Constraint":
        return self.constraints[constraint_cls]

    def __getattr__(self, attr: str):
        for ctr in self.constraints.values():
            if attr in ctr.__dict__:
                return ctr.__dict__[attr]

        raise AttributeError("'{}' task has no attribute '{}'".format(self.name, attr))

    def set(self, constraint: "Constraint") -> "Task":
        self.constraints[type(constraint)] = constraint

        return self


class Chunk:
    @property
    def completion_time(self):
        return self.start_time + max(ptime for ptime in self.proc_times.values())

    def __init__(self, task: "Task", start_time: float, proc_times: Dict[Any, float]):
        self.task = task
        self.start_time = start_time
        self.proc_times = proc_times

    def is_valid(self, schedule: "Schedule") -> bool:
        for ctr in self.task.constraints.values():
            if not ctr.is_valid(schedule, self):
                return False

        return True

    def append_to(self, schedule: "Schedule"):
        for ctr in self.task.constraints.values():
            if not ctr.is_valid(schedule, self):
                raise ConstraintError(ctr.get_error(schedule, self))

        if self.task in schedule:
            schedule[self.task].append(self)
        else:
            schedule[self.task] = [self]


class Schedule:
    def __init__(self, chunk_map: Optional[Dict["Task", List["Chunk"]]] = None):
        if chunk_map is None:
            self.chunk_map = {}
        else:
            self.chunk_map = chunk_map

    def __getitem__(self, task: "Task") -> List["Chunk"]:
        return self.chunk_map[task]

    def __setitem__(self, task: "Task", chunks: List["Chunk"]):
        self.chunk_map[task] = chunks

    def __contains__(self, task: "Task") -> bool:
        return task in self.chunk_map

    def __iter__(self) -> Iterator["Task"]:
        return iter(self.chunk_map)

    def __len__(self) -> int:
        return len(self.chunk_map)

    def copy(self):
        chunk_map = self.chunk_map.copy()

        for tsk in chunk_map:
            chunk_map[tsk] = chunk_map[tsk].copy()

        return Schedule(chunk_map)

    def get(self, prop: "Property") -> Any:
        return prop.get(self)

    def apply(self, operator: "Operator") -> "Schedule":
        operator.apply(self)

        return self
