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
    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        pass

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' constraint is not met".format(type(self).__name__)


class Property(metaclass=ABCMeta):
    @abstractmethod
    def get(self, schedule: "Schedule") -> Any:
        pass


class Operator(metaclass=ABCMeta):
    @abstractmethod
    def apply(self, schedule: "Schedule") -> Any:
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
    def __init__(self, task: "Task", start_time: float, proctimes: Dict[Any, float]):
        self.task = task
        self.start_time = start_time
        self.proctimes = proctimes

    def completion_time(self, node: Any) -> float:
        if node in self.proctimes:
            return self.start_time + self.proctimes[node]
        else:
            return inf

    def isvalid(self, schedule: "Schedule") -> bool:
        for ctr in self.task.constraints.values():
            if not ctr.isvalid(schedule, self):
                return False

        return True

    def append_to(self, schedule: "Schedule"):
        for ctr in self.task.constraints.values():
            if not ctr.isvalid(schedule, self):
                raise ConstraintError(ctr.geterror(schedule, self))

        if self.task in schedule.taskmap:
            schedule.taskmap[self.task].append(self)
        else:
            schedule.taskmap[self.task] = [self]

        for node in self.proctimes:
            if node in schedule.nodemap:
                schedule.nodemap[node].add(self)
            else:
                schedule.nodemap[node] = ChunkTree(node).add(self)

    def remove_from(self, schedule: "Schedule"):
        schedule.taskmap[self.task].remove(self)

        for node in self.proctimes:
            schedule.nodemap[node].remove(self)


class ChunkNode:
    def __init__(self, chunk: "Chunk"):
        self.chunk = chunk
        self.height = 1
        self.hi = -inf
        self.left = None
        self.right = None


class ChunkTree:
    def __init__(self, node: Any):
        self.node = node
        self.root = None

    def __iter__(self) -> Optional[Iterator["ChunkNode"]]:
        return self._iter_from(self.root)

    def _iter_from(self, root: Optional["ChunkNode"]) -> Optional[Iterator["ChunkNode"]]:
        if root is None:
            return None
        else:
            yield from self._iter_from(root.left)
            yield root
            yield from self._iter_from(root.right)

    def at(self, time: float) -> List["ChunkNode"]:
        nodes = []

        self._at_from(self.root, time, nodes)

        return nodes

    def _at_from(self, root: Optional["ChunkNode"], time: float, nodes: List["ChunkNode"]):
        if root is not None:
            if root.left is not None and time < root.left.hi:
                self._at_from(root.left, time, nodes)

            if root.chunk.start_time <= time < root.chunk.completion_time(self.node):
                nodes.append(root)

            self._at_from(root.right, time, nodes)

    def over(self, lo: float, hi: float) -> List["ChunkNode"]:
        nodes = []

        self._over_from(self.root, lo, hi, nodes)

        return nodes

    def _over_from(self, root: Optional["ChunkNode"], lo: float, hi: float, nodes: List["ChunkNode"]):
        if root is not None:
            if root.left is not None and lo < root.left.hi:
                self._over_from(root.left, lo, hi, nodes)

            if lo < root.chunk.completion_time(self.node) and root.chunk.start_time < hi:
                nodes.append(root)

            self._over_from(root.right, lo, hi, nodes)

    def add(self, chunk: "Chunk") -> "ChunkTree":
        self.root = self._add_from(self.root, chunk)

        return self

    def _add_from(self, root: Optional["ChunkNode"], chunk: "Chunk") -> "ChunkNode":
        if root is None:
            treenode = ChunkNode(chunk)

            treenode.hi = chunk.completion_time(self.node)

            return treenode
        else:
            if chunk.start_time < root.chunk.start_time:
                root.left = self._add_from(root.left, chunk)
            else:
                root.right = self._add_from(root.right, chunk)

            root.height = 1 + max(self._height(root.left), self._height(root.right))
            root.hi = max(self._hi(root), chunk.completion_time(self.node))

            return self._rotate(root)

    def remove(self, chunk: "Chunk") -> "ChunkTree":
        self.root = self._remove_from(self.root, chunk)

        return self

    def _remove_from(self, root: Optional["ChunkNode"], chunk: "Chunk") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            if chunk.start_time < root.chunk.start_time:
                root.left = self._remove_from(root.left, chunk)
            elif chunk.start_time > root.chunk.start_time:
                root.right = self._remove_from(root.right, chunk)
            else:
                if root.left is None:
                    return root.right
                elif root.right is None:
                    return root.left
                else:
                    successor = self._min_from(root.right)

                    root.chunk = successor.chunk

                    root.right = self._remove_from(root.right, successor.chunk)

            root.height = 1 + max(self._height(root.left), self._height(root.right))
            root.hi = max(root.chunk.completion_time(self.node), self._hi(root.left), self._hi(root.right))

            return self._rotate(root)

    def _rotate(self, root: "ChunkNode") -> "ChunkNode":
        balance = self._balance(root)

        if balance > 1 and self._balance(root.left) >= 0:
            return self._rotate_right(root)
        elif balance > 1 and self._balance(root.left) < 0:
            root.left = self._rotate_left(root.left)

            return self._rotate_right(root)
        elif balance < -1 and self._balance(root.right) <= 0:
            return self._rotate_left(root)
        elif balance < -1 and self._balance(root.right) > 0:
            root.right = self._rotate_right(root.right)

            return self._rotate_left(root)
        else:
            return root

    def _rotate_left(self, root: "ChunkNode") -> "ChunkNode":
        pivot = root.right
        child = pivot.left

        pivot.left = root
        root.right = child

        root.height = 1 + max(self._height(root.left), self._height(root.right))
        root.hi = max(root.chunk.completion_time(self.node), self._hi(root.left), self._hi(root.right))

        pivot.height = 1 + max(self._height(pivot.left), self._height(pivot.right))
        pivot.hi = max(pivot.chunk.completion_time(self.node), self._hi(pivot.left), self._hi(pivot.right))

        return pivot

    def _rotate_right(self, root: "ChunkNode") -> "ChunkNode":
        pivot = root.left
        child = pivot.right

        pivot.right = root
        root.left = child

        root.height = 1 + max(self._height(root.left), self._height(root.right))
        root.hi = max(root.chunk.completion_time(self.node), self._hi(root.left), self._hi(root.right))

        pivot.height = 1 + max(self._height(pivot.left), self._height(pivot.right))
        pivot.hi = max(pivot.chunk.completion_time(self.node), self._hi(pivot.left), self._hi(pivot.right))

        return pivot

    def _balance(self, root: "ChunkNode") -> int:
        if root is None:
            return 0
        else:
            return self._height(root.left) - self._height(root.right)

    def _height(self, root: "ChunkNode") -> int:
        if root is None:
            return 0
        else:
            return root.height

    def _hi(self, root: Optional["ChunkNode"]) -> float:
        if root is None:
            return -inf
        else:
            return root.hi

    def min(self) -> Optional["ChunkNode"]:
        return self._min_from(self.root)

    def _min_from(self, root: "ChunkNode") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            current = root

            while current.left is not None:
                current = current.left

            return current

    def max(self) -> Optional["ChunkNode"]:
        return self._max_from(self.root)

    def _max_from(self, root: "ChunkNode") -> Optional["ChunkNode"]:
        if root is None:
            return None
        else:
            current = root

            while current.right is not None:
                current = current.right

            return current


class Schedule:
    def __init__(self):
        self.taskmap = {}
        self.nodemap = {}

    def tasks(self) -> Iterator["Task"]:
        return iter(self.taskmap)

    def hastask(self, task: "Task") -> bool:
        return task in self.taskmap

    def task(self, task: "Task") -> Optional[List["Chunk"]]:
        if task in self.taskmap:
            return self.taskmap[task]
        else:
            return None

    def nodes(self) -> Iterator[Any]:
        return iter(self.nodemap)

    def hasnode(self, node: Any) -> bool:
        return node in self.nodemap

    def node(self, node: Any) -> Optional["ChunkTree"]:
        if node in self.nodemap:
            return self.nodemap[node]
        else:
            return None

    # def copy(self):
    #     chunk_map = self.taskmap.copy()
    #
    #     for tsk in chunk_map:
    #         chunk_map[tsk] = chunk_map[tsk].copy()
    #
    #     return Schedule(chunk_map)

    def get(self, prop: "Property") -> Any:
        return prop.get(self)

    def apply(self, operator: "Operator") -> Any:
        return operator.apply(self)
