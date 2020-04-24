from typing import Optional, Any, Dict, List

from dstf.core import Property, Schedule, Task, Chunk


class ChunksAtProperty(Property):
    def __init__(self, time: float):
        self.time = time

    def get(self, schedule: "Schedule") -> List["Chunk"]:
        chks = []

        for node in schedule.nodes():
            tree = schedule.node(node)
            treenode = tree.get(self.time)

            if treenode is not None:
                chks.append(treenode.chunk)

        return chks


class ProcessedTimesProperty(Property):
    def __init__(self, task: "Task"):
        self.task = task

    def get(self, schedule: "Schedule") -> Optional[Dict[Any, float]]:
        if schedule.hastask(self.task):
            prcs_times = {}

            for chk in schedule.task(self.task):
                for node, ptime in chk.proctimes.items():
                    if node in prcs_times:
                        prcs_times[node] += ptime
                    else:
                        prcs_times[node] = ptime

            return prcs_times
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
