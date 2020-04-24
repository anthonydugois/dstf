from dstf.core import Operator, Schedule, Chunk
from dstf.properties import ChunksAtProperty


class AppendOperator(Operator):
    def apply(self, schedule: "Schedule"):
        Chunk(self.task, self.start_time, self.proc_times).append_to(schedule)


class PreemptOperator(Operator):
    def apply(self, schedule: "Schedule"):
        chks = schedule.get(ChunksAtProperty(self.start_time))

        for chk in chks:
            proctimes = chk.proc_times.copy()

            for node, ptime in self.proc_times.items():
                if node in proctimes and chk.start_time <= self.start_time < chk.start_time + chk.proc_times[node]:
                    if self.start_time <= chk.start_time:
                        del proctimes[node]
                    else:
                        proctimes[node] = self.start_time - chk.start_time

            if proctimes != chk.proc_times:
                chk.remove_from(schedule)

                if proctimes:
                    Chunk(chk.task, chk.start_time, proctimes).append_to(schedule)

        Chunk(self.task, self.start_time, self.proc_times).append_to(schedule)
