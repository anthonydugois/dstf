from typing import Any, List, Dict

from dstf.core import Constraint, Schedule, Chunk


class NoSimultaneousExecutionConstraint(Constraint):
    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node in chunk.proctimes:
            if schedule.hasnode(node):
                tree = schedule.node(node)
                treenodes = tree.over(chunk.start_time, chunk.completion_time(node))

                if len(treenodes) > 0:
                    return False

        return True

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task cannot run on a busy node".format(chunk.task.name)


class NoMigrationConstraint(Constraint):
    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        if schedule.hastask(chunk.task):
            for chk in schedule.task(chunk.task):
                if not set(chk.proctimes).issuperset(chunk.proctimes):
                    return False

        return True

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        pass


class ProcessingTimesConstraint(Constraint):
    def __init__(self, processing_times: Dict[Any, float]):
        self.processing_times = processing_times

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node, ptime in chunk.proctimes.items():
            processed = 0

            if schedule.hastask(chunk.task):
                for chk in schedule.task(chunk.task):
                    if node in chk.proctimes:
                        processed += chk.proctimes[node]

            if ptime > self.processing_times[node] - processed:
                return False

        return True

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task cannot run longer than {}".format(chunk.task.name, self.processing_times)


class ReleaseTimeConstraint(Constraint):
    def __init__(self, release_time: float):
        self.release_time = release_time

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return chunk.start_time >= self.release_time

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task cannot start before {}".format(chunk.task.name, self.release_time)


class DeadlineConstraint(Constraint):
    def __init__(self, deadline: float):
        self.deadline = deadline

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node, ptime in chunk.proctimes.items():
            if chunk.start_time + ptime > self.deadline:
                return False

        return True

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task cannot finish after {}".format(chunk.task.name, self.deadline)


class MultipurposeMachinesConstraint(Constraint):
    def __init__(self, compatible_nodes: List[Any]):
        self.compatible_nodes = compatible_nodes

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node in chunk.proctimes:
            if node not in self.compatible_nodes:
                return False

        return True

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task should be processed by a subset of {}".format(chunk.task.name, self.compatible_nodes)


class ExecutionSizeConstraint(Constraint):
    def __init__(self, execution_size: int):
        self.execution_size = execution_size

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return len(chunk.proctimes) == self.execution_size

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task should be processed by {} nodes".format(chunk.task.name, self.execution_size)


class ExecutionNodesConstraint(Constraint):
    def __init__(self, execution_nodes: List[Any]):
        self.execution_nodes = execution_nodes

    def isvalid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return set(chunk.proctimes) == set(self.execution_nodes)

    def geterror(self, schedule: "Schedule", chunk: "Chunk") -> str:
        return "'{}' task should be processed by {}".format(chunk.task.name, self.execution_nodes)
