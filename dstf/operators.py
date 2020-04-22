from dstf.core import Operator, Schedule, Chunk


class AppendOperator(Operator):
    def apply(self, schedule: "Schedule"):
        Chunk(self.task, self.start_time, self.proc_times).append_to(schedule)


class PreemptOperator(Operator):
    def apply(self, schedule: "Schedule"):
        for tsk in schedule:
            for chk in schedule[tsk]:
                proc_times = chk.proc_times.copy()

                for node, ptime in self.proc_times.items():
                    if node in proc_times and chk.start_time <= self.start_time < chk.start_time + chk.proc_times[node]:
                        if self.start_time <= chk.start_time:
                            del proc_times[node]
                        else:
                            proc_times[node] = self.start_time - chk.start_time

                if proc_times != chk.proc_times:
                    schedule[tsk].remove(chk)

                    if proc_times:
                        Chunk(chk.task, chk.start_time, proc_times).append_to(schedule)

        Chunk(self.task, self.start_time, self.proc_times).append_to(schedule)
