from typing import List, Dict

from dstf.core import Constraint, Schedule, Chunk, Node
from dstf.properties import processed_times


class IdleConstraint(Constraint):
    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for tsk in schedule:
            for chk in schedule[tsk]:
                for node, ptime in chunk.proc_times.items():
                    if (node in chk.proc_times
                            and chunk.start_time < chk.start_time + chk.proc_times[node]
                            and chunk.start_time + ptime > chk.start_time):
                        return False

        return True


class ProcessingTimesConstraint(Constraint):
    def __init__(self, processing_times: Dict["Node", float]):
        self.processing_times = processing_times

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        prcs_times = processed_times(schedule, chunk.task)

        for node, ptime in chunk.proc_times.items():
            if prcs_times is None:
                rmn_ptime = self.processing_times[node]
            else:
                rmn_ptime = self.processing_times[node] - prcs_times[node]

            if ptime > rmn_ptime:
                return False

        return True


class ReleaseTimeConstraint(Constraint):
    def __init__(self, release_time: float):
        self.release_time = release_time

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return chunk.start_time >= self.release_time


class DeadlineConstraint(Constraint):
    def __init__(self, deadline: float):
        self.deadline = deadline

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node, ptime in chunk.proc_times.items():
            if chunk.start_time + ptime > self.deadline:
                return False

        return True


class MultipurposeMachinesConstraint(Constraint):
    def __init__(self, compatible_nodes: List["Node"]):
        self.compatible_nodes = compatible_nodes

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        for node in chunk.proc_times:
            if node not in self.compatible_nodes:
                return False

        return True


class ExecutionSizeConstraint(Constraint):
    def __init__(self, execution_size: int):
        self.execution_size = execution_size

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return len(chunk.proc_times) == self.execution_size


class ExecutionNodesConstraint(Constraint):
    def __init__(self, execution_nodes: List["Node"]):
        self.execution_nodes = execution_nodes

    def is_valid(self, schedule: "Schedule", chunk: "Chunk") -> bool:
        return set(chunk.proc_times) == set(self.execution_nodes)
