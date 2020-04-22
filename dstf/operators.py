from dstf.core import Operator, Schedule, Chunk


class AppendOperator(Operator):
    def apply(self, schedule: "Schedule") -> "Schedule":
        chunk_map = schedule.chunk_map.copy()

        for tsk in chunk_map:
            chunk_map[tsk] = chunk_map[tsk].copy()

        return Schedule(chunk_map).append(self.task, self.start_time, self.proc_times)


class PreemptOperator(Operator):
    def apply(self, schedule: "Schedule") -> "Schedule":
        chunk_map = schedule.chunk_map.copy()

        for tsk in chunk_map:
            chunk_map[tsk] = chunk_map[tsk].copy()

            for chk in chunk_map[tsk]:
                proc_times = chk.proc_times.copy()

                for node, ptime in self.proc_times.items():
                    if node in proc_times and chk.start_time <= self.start_time < chk.start_time + proc_times[node]:
                        if self.start_time <= chk.start_time:
                            del proc_times[node]
                        else:
                            proc_times[node] = self.start_time - chk.start_time

                if proc_times != chk.proc_times:
                    chunk_map[tsk].remove(chk)

                    if proc_times:
                        chunk_map[tsk].append(Chunk(chk.task, chk.start_time, proc_times))

        return Schedule(chunk_map).append(self.task, self.start_time, self.proc_times)
