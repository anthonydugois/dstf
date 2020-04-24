from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from math import inf
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
    def __init__(self, task: "Task", start_time: float, proc_times: Dict[Any, float]):
        self.task = task
        self.start_time = start_time
        self.proc_times = proc_times

    def completion_time(self, node: Any) -> float:
        if node in self.proc_times:
            return self.start_time + self.proc_times[node]
        else:
            return inf

    def is_valid(self, schedule: "Schedule") -> bool:
        for ctr in self.task.constraints.values():
            if not ctr.is_valid(schedule, self):
                return False

        return True

    def append_to(self, schedule: "Schedule"):
        for ctr in self.task.constraints.values():
            if not ctr.is_valid(schedule, self):
                raise ConstraintError(ctr.get_error(schedule, self))

        if self.task in schedule.chunk_map:
            schedule.chunk_map[self.task].append(self)
        else:
            schedule.chunk_map[self.task] = [self]

        for node in self.proc_times:
            if node in schedule.nodemap:
                schedule.nodemap[node].add(self)
            else:
                schedule.nodemap[node] = ChunkTree(node).add(self)

    def remove_from(self, schedule: "Schedule"):
        schedule.chunk_map[self.task].remove(self)

        for node in self.proc_times:
            schedule.nodemap[node].remove(self)


class ChunkNode:
    def __init__(self, chunk: "Chunk"):
        self.chunk = chunk
        self.height = 1
        self.left = None
        self.right = None


class ChunkTree:
    def __init__(self, node: Any):
        self.node = node
        self.root = None

    def __iter__(self) -> Optional[Iterator["ChunkNode"]]:
        return self.iter_from(self.root)

    def iter_from(self, root: Optional["ChunkNode"]) -> Optional[Iterator["ChunkNode"]]:
        if root is None:
            return None
        else:
            yield from self.iter_from(root.left)
            yield root
            yield from self.iter_from(root.right)

    def get(self, time: float) -> Optional["ChunkNode"]:
        return self.get_from(self.root, time)

    def get_from(self, root: "ChunkNode", time: float) -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            if root.chunk.start_time <= time < root.chunk.completion_time(self.node):
                return root
            elif time < root.chunk.start_time:
                return self.get_from(root.left, time)
            elif time >= root.chunk.completion_time(self.node):
                return self.get_from(root.right, time)
            else:
                return None

    def add(self, chunk: "Chunk") -> "ChunkTree":
        self.root = self.add_from(self.root, chunk)

        return self

    def add_from(self, root: "ChunkNode", chunk: "Chunk") -> "ChunkNode":
        if root is None:
            return ChunkNode(chunk)
        else:
            if chunk.completion_time(self.node) <= root.chunk.start_time:
                root.left = self.add_from(root.left, chunk)
            elif chunk.start_time >= root.chunk.completion_time(self.node):
                root.right = self.add_from(root.right, chunk)
            else:
                return root

            root.height = 1 + max(self.height(root.left), self.height(root.right))

            return self.rotate(root)

    def remove(self, chunk: "Chunk") -> "ChunkTree":
        self.root = self.remove_from(self.root, chunk)

        return self

    def remove_from(self, root: "ChunkNode", chunk: "Chunk") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            if chunk.completion_time(self.node) <= root.chunk.start_time:
                root.left = self.remove_from(root.left, chunk)
            elif chunk.start_time >= root.chunk.completion_time(self.node):
                root.right = self.remove_from(root.right, chunk)
            else:
                if root.left is None:
                    return root.right
                elif root.right is None:
                    return root.left
                else:
                    successor = self.min_from(root.right)

                    root.chunk = successor.chunk

                    root.right = self.remove_from(root.right, successor.chunk)

            root.height = 1 + max(self.height(root.left), self.height(root.right))

            return self.rotate(root)

    def rotate(self, root: "ChunkNode") -> "ChunkNode":
        balance = self.balance(root)

        if balance > 1 and self.balance(root.left) >= 0:
            return self.rotate_right(root)
        elif balance > 1 and self.balance(root.left) < 0:
            root.left = self.rotate_left(root.left)

            return self.rotate_right(root)
        elif balance < -1 and self.balance(root.right) <= 0:
            return self.rotate_left(root)
        elif balance < -1 and self.balance(root.right) > 0:
            root.right = self.rotate_right(root.right)

            return self.rotate_left(root)
        else:
            return root

    def rotate_left(self, root: "ChunkNode") -> "ChunkNode":
        pivot = root.right
        child = pivot.left

        pivot.left = root
        root.right = child

        root.height = 1 + max(self.height(root.left), self.height(root.right))
        pivot.height = 1 + max(self.height(pivot.left), self.height(pivot.right))

        return pivot

    def rotate_right(self, root: "ChunkNode") -> "ChunkNode":
        pivot = root.left
        child = pivot.right

        pivot.right = root
        root.left = child

        root.height = 1 + max(self.height(root.left), self.height(root.right))
        pivot.height = 1 + max(self.height(pivot.left), self.height(pivot.right))

        return pivot

    def balance(self, root: "ChunkNode") -> int:
        if root is None:
            return 0
        else:
            return self.height(root.left) - self.height(root.right)

    def height(self, root: "ChunkNode") -> int:
        if root is None:
            return 0
        else:
            return root.height

    def min(self) -> Optional["ChunkNode"]:
        return self.min_from(self.root)

    def min_from(self, root: "ChunkNode") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            current = root

            while current.left is not None:
                current = current.left

            return current

    def max(self) -> Optional["ChunkNode"]:
        return self.max_from(self.root)

    def max_from(self, root: "ChunkNode") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            current = root

            while current.right is not None:
                current = current.right

            return current


class Schedule:
    def __init__(self, chunk_map: Optional[Dict["Task", List["Chunk"]]] = None):
        if chunk_map is None:
            self.chunk_map = {}
        else:
            self.chunk_map = chunk_map

        self.nodemap = {}

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
