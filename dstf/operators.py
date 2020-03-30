from dstf.core import Operator, Schedule


class AppendOperator(Operator):
    def apply(self, schedule: "Schedule") -> "Schedule":
        next_sched = Schedule()

        for tsk in schedule:
            for chk in schedule[tsk]:
                next_sched.append(chk.task, chk.start_time, chk.proc_times)

        next_sched.append(self.task, self.start_time, self.proc_times)

        return next_sched


class PreemptOperator(Operator):
    def apply(self, schedule: "Schedule") -> "Schedule":
        next_sched = Schedule()

        for tsk in schedule:
            for chk in schedule[tsk]:
                proc_times = dict(chk.proc_times)

                for node, ptime in self.proc_times.items():
                    if node in proc_times and chk.start_time <= self.start_time < chk.start_time + proc_times[node]:
                        if self.start_time <= chk.start_time:
                            del proc_times[node]
                        else:
                            proc_times[node] = self.start_time - chk.start_time

                if proc_times:
                    next_sched.append(chk.task, chk.start_time, proc_times)

        next_sched.append(self.task, self.start_time, self.proc_times)

        return next_sched
