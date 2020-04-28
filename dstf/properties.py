from typing import Optional, Any, Dict, Set

from dstf.core import Property, Schedule, Task, Chunk


class ChunksAtProperty(Property):
    def __init__(self, time: float):
        self.time = time

    def get(self, schedule: "Schedule") -> Set["Chunk"]:
        chks = set()

        for node in schedule.nodes():
            tree = schedule.node(node)
            treenodes = tree.at(self.time)

            for treenode in treenodes:
                chks.add(treenode.chunk)

        return chks


class ChunksOverProperty(Property):
    def __init__(self, lo: float, hi: float):
        self.lo = lo
        self.hi = hi

    def get(self, schedule: "Schedule") -> Set["Chunk"]:
        chks = set()

        for node in schedule.nodes():
            tree = schedule.node(node)
            treenodes = tree.over(self.lo, self.hi)

            for treenode in treenodes:
                chks.add(treenode.chunk)

        return chks


class ProcessedTimesProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[Dict[Any, float]]:
        if schedule.hastask(self.task):
            processed = {}

            for chk in schedule.task(self.task):
                for node, ptime in chk.proctimes.items():
                    if node in processed:
                        processed[node] += ptime
                    else:
                        processed[node] = ptime

            return processed
        else:
            return None


class StartTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[float]:
        if schedule.hastask(self.task):
            return min(chk.start_time for chk in schedule.task(self.task))
        else:
            return None


class CompletionTimeProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[float]:
        if schedule.hastask(self.task):
            return max(max(chk.completion_time(node) for node in chk.proctimes) for chk in schedule.task(self.task))
        else:
            return None
