from dstf.core import Operator, Schedule, Chunk
from dstf.properties import ChunksAtProperty


class AppendOperator(Operator):
    def __init__(self, chunk: "Chunk"):
        self.chunk = chunk

    def apply(self, schedule: "Schedule"):
        self.chunk.append_to(schedule)


class RemoveOperator(Operator):
    def __init__(self, chunk: "Chunk"):
        self.chunk = chunk

    def apply(self, schedule: "Schedule"):
        self.chunk.remove_from(schedule)


class PreemptOperator(Operator):
    def __init__(self, chunk: "Chunk"):
        self.chunk = chunk

    def apply(self, schedule: "Schedule"):
        chks = schedule.get(ChunksAtProperty(self.chunk.start_time))

        for chk in chks:
            proctimes = chk.proctimes.copy()

            for node in proctimes:
                if (node in self.chunk.proctimes
                        and self.chunk.start_time < chk.completion_time(node)
                        and chk.start_time < self.chunk.start_time + self.chunk.proctimes[node]):
                    if self.chunk.start_time <= chk.start_time:
                        del proctimes[node]
                    else:
                        proctimes[node] = self.chunk.start_time - chk.start_time

            if proctimes != chk.proctimes:
                chk.remove_from(schedule)

                if proctimes:
                    Chunk(chk.task, chk.start_time, proctimes).append_to(schedule)

        self.chunk.append_to(schedule)
