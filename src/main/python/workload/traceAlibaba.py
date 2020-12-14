#!/usr/bin/env python3
# coding=utf-8
import argparse
import os
import re
from typing import Dict, List, Any, Tuple


# Alibaba 2018 trace
#  49G alibaba_clusterdata_v2018.tar.gz
#  20G batch_instance.tar.gz
# 125M batch_task.tar.gz
# 2.4M container_meta.tar.gz
#  28G container_usage.tar.gz
#  92K machine_meta.tar.gz
# 1.7G machine_usage.tar.gz
# ### batch task
# +------------------------------------------------------------------------------------+
# | task_name       | string     |       | task name. unique within a job              |
# | instance_num    | bigint     |       | number of instances                         |
# | job_name        | string     |       | job name                                    |
# | task_type       | string     |       | task type                                   |
# | status          | string     |       | task status                                 |
# | start_time      | bigint     |       | start time of the task                      |
# | end_time        | bigint     |       | end of time the task                        |
# | plan_cpu        | double     |       | number of cpu needed by the task, 100 is 1 core |
# | plan_mem        | double     |       | normalized memorty size, [0, 100]           |
# +------------------------------------------------------------------------------------+
#
# ### container meta
# +------------------------------------------------------------------------------------+
# | Field           | Type       | Label | Comment                                     |
# +------------------------------------------------------------------------------------+
# | container_id    | string     |       | uid of a container                          |
# | machine_id      | string     |       | uid of container's host machine             |
# | time_stamp      | bigint     |       |                                             |
# | app_du          | string     |       | containers with same app_du belong to same application group |
# | status          | string     |       |                                             |
# | cpu_request     | bigint     |       | 100 is 1 core                               |
# | cpu_limit       | bigint     |       | 100 is 1 core                               |
# | mem_size        | double     |       | normarlized memory, [0, 100]                |
# +------------------------------------------------------------------------------------+
#
# * about app_du: Containers belong to the same deploy unit provides one service, typically, they should be spread across failure domains
#
# ### machine meta
# +------------------------------------------------------------------------------------+
# | Field           | Type       | Label | Comment                                     |
# +------------------------------------------------------------------------------------+
# | machine_id      | string     |       | uid of machine                              |
# | time_stamp      | bigint     |       | time stamp, in second                       |
# | failure_domain_1 | bigint     |       | one level of container failure domain      |
# | failure_domain_2 | string     |       | another level of container failure domain  |
# | cpu_num         | bigint     |       | number of cpu on a machine                  |
# | mem_size        | bigint     |       | normalized memory size. [0, 100]            |
# | status          | string     |       | status of a machine                         |
# +------------------------------------------------------------------------------------+
#
# * task name indicates the DAG information, see the explanation of batch workloads
# * Task name is uniqe within a job; note task name indicates the DAG information, see the explanation of batch workloads
# * There are totally 12 types, and only some of them have DAG info
#
#
# Some explanation of common fields.
#
# time_stamp, start_time and end_time: These fields in the tables are all with unit "second" and the number if the difference of between the actual time and the beginning of the period over which the trace is sampled. The beginning of period is 0.
# For confidential reason, we normalize some values such as memory size and disk size and rescale such field between [0, 100]. However, there are some invalid values and they are set to either -1 or 101.
#
# In this version of cluster data, we include many types of batch wokrloads. Most of them are DAGs while some of them are not. For those tasks that are not DAGs, we name them using random characters, such as task_Nzg3ODAwNDgzMTAwNTc2NTQ2Mw== or task_Nzg3ODAwNDgzMTAwODc2NTQ3MQ==. These tasks can be treated as independent tasks. In the remainder of this section, we explain how to deduce DAG of a job from the task names.


class ServerMachine:
    
    def __init__(self, id, cpu, mem, fd_l1, fd_l2):
        self.id = id
        self.cpu = cpu
        self.mem = mem
        self.fd_l1 = fd_l1
        self.fd_l2 = fd_l2
        
        self.status: List[Tuple[int, bool]] = []
    
    def addEvent(self, t, new_available_status):
        self.status.append((t, new_available_status))
    
    def parse(self):
        if self.cpu is None or self.cpu < 0 or self.mem is None or self.mem < 0:
            raise NameError("invalid resource constraints")
        
        self.status = list(sorted(self.status, key=lambda e: e[0]))
        if len(self.status) == 0:
            raise NameError("no status event exists")
        
        i = 0
        last_state = self.status[0][1]
        while i < len(self.status) - 1:
            if i > 0:
                if self.status[i][1] == last_state:
                    # remove this
                    del self.status[i]
                    continue
                    
            last_state = self.status[i][1]
            i += 1


class TaskGroup:
    
    def __init__(self, name, instances, cpu, mem, start_time, end_time, dependencies):
        self.name = name
        self.first_name = None
        self.instances = instances
        self.cpu = cpu
        self.mem = mem
        self.time_start = start_time
        self.time_end = end_time
        self.dependencies = dependencies
        
        self.duration = None
        self.connect_to = []
    
    def parse(self, job_submit_time, last_time):
        if self.instances is None:
            raise NameError("no instance count given")
        if self.cpu is None or self.cpu <= 0:
            raise NameError("no CPU res given")
        if self.mem is None or self.mem <= 0:
            raise NameError("no mem res given")
        if self.mem < 0 or self.cpu < 0 or self.mem > 100 or self.cpu > 96*100: # there are at most 96 cores, with a 100 scaling
            raise NameError("invalid resources")
        if self.time_start is None:
            self.time_start = job_submit_time
        if self.time_end is None or self.time_end == 0:
            self.time_end = last_time  # if there is not end time, task runs till end of trace
        if self.time_end == self.time_start:
            self.time_end += 1  # less than 1 second (trace has a granualirty of only a seoncd)
        
        self.duration = self.time_end - self.time_start
        if self.duration <= 0:
            raise NameError(f"duration is smaller 0! start:{self.time_start} end:{self.time_end}")

    def __repr__(self):
        return f"TG({self.name}/{self.first_name}/{self.instances}/{self.cpu}/{self.mem}/" \
            f"{self.dependencies}/" \
            f"{self.time_start}/{self.time_end}/{self.duration}/" \
            f"{self.connect_to})"


class Job(object):
    
    def __init__(self, id, job_type):
        self.id = id
        self.job_type = job_type
        self.task_groups: Dict[str, TaskGroup] = {}
        
        self.submit_time = None
    
    def parse(self, last_time):
        if not (len(self.task_groups) > 0):
            raise NameError("no TGs in job")
        self.submit_time = [tg.time_start for tg in self.task_groups.values() if tg.time_start is not None]
        self.submit_time = 0 if len(self.submit_time) == 0 else min(self.submit_time)
        for tg in self.task_groups.values():
            tg.parse(self.submit_time, last_time)
        
        for tg in self.task_groups.values():
            if tg.time_start < self.submit_time:
                raise NameError("should not happen, but task group starts before the job")
        
        for tg in self.task_groups.values():
            for dep in tg.dependencies:
                before_tg = f"{tg.first_name}{dep}"
                # dataset is not fully complete, so dependencies are missing
                if before_tg in self.task_groups:
                    self.task_groups[before_tg].connect_to.append(tg.name)
                # else:
                #     print(f"missing {before_tg} in {self.task_groups.keys()}")
    
    def addTaskGroup(self, id, instances, cpu, mem, start_time, end_time, dependencies):
        assert id not in self.task_groups
        self.task_groups[id] = TaskGroup(id, instances, cpu, mem, start_time, end_time, dependencies)
    
    def __str__(self):
        return f"Job({self.id} @ {self.submit_time}, {self.task_groups})"
    
    def __repr__(self):
        return str(self)


def warn(msg):
    print(f"WARNING: {msg}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--parse-machines", action="store_true", default=False)
    parser.add_argument("--parse-jobs", action="store_true", default=False)
    parser.add_argument("--folder", type=str, default="traces/alibaba_trace_2018")
    args = parser.parse_args()
    
    batch_task_file = os.path.join(args.folder, "batch_task.csv")
    machines_file = os.path.join(args.folder, "machine_meta.csv")
    service_container_file = os.path.join(args.folder, "container_meta.csv")
    target_job_file = os.path.join(args.folder, "hire_workload_jobs.csv")
    target_cluster_desc = os.path.join(args.folder, "hire_cluster.csv")
    
    if args.parse_machines:
        print("parse machines")
        
        machines: Dict[str, ServerMachine] = {}
        
        print(f"Start parsing machines: {machines_file}")
        i = 0
        parsed_rows = 0
        with open(machines_file, "r") as f:
            for row in f:
                i += 1
                if i % 500000 == 0:
                    print(".", end="", flush=True)

                cols = list(map(str.strip, row.split(",")))
                
                assert len(cols) == 7
                try:
                    machine = cols[0]
                    timestamp = int(cols[1])
                    fd_1 = cols[2]
                    fd_2 = cols[3]
                    
                    cpu_res = int(cols[4])
                    mem_res = float(cols[5])
                    
                    status = cols[6]
                    
                    if machine not in machines:
                        machines[machine] = ServerMachine(machine, cpu_res, mem_res, fd_1, fd_2)
                    
                    if status == "USING":
                        machines[machine].addEvent(timestamp, True)
                    elif status == "IMPORT_INSTALLING":
                        machines[machine].addEvent(timestamp, False)
                    else:
                        print(status)  # not yet implemented
                        break
                    
                    parsed_rows += 1
                except BaseException as e:
                    print(f"error while processing row: {cols}, {e}")
                    raise e
        print(f"parsed rows:{parsed_rows} out of {i}")
        
        failed_m = 0
        passed_m = 0
        
        valid_machines: List[ServerMachine] = []
        
        print("perform sanity checks")
        for m in machines.values():
            try:
                m.parse()
                passed_m += 1
                
                valid_machines.append(m)
            except BaseException as e:
                print(e)
                failed_m += 1
        
        print(f"machine parsing: {failed_m} failed, {passed_m} passed")
        
        sep = ","
        j: Job = None
        with open(target_cluster_desc, "w") as f:
            f.write(sep.join(["machine", "cpu", "mem", "failure_d_1", "failure_d_2", "available", "time"]))
            k = 0
            m: ServerMachine = None
            for m in valid_machines:
                for status in m.status:
                    f.write("\n")
                    k += 1
                    f.write(sep.join(map(str, [m.id, m.cpu, m.mem, m.fd_l1, m.fd_l2, status[1], status[0]])))
                if k > 10000:
                    f.flush()
                    k = 0
            
            f.flush()
    
    if args.parse_jobs:
        print("parse jobs")
        
        jobs_ok: List[Tuple[str, Any]] = []  # holds pairs of job type and instance job/container
        jobs: Dict[str, Job] = {}
        
        task_re_independent = re.compile(r"(task_[\S]*)|(MergeTask)")
        task_re_dependent = re.compile(
            r"([a-zA-Z]+)([0-9]+)_?(_[0-9]+)*_?(Stg[0-9]+)?")  # groups char of task, then int, then dependencies
        
        # Some task names end with Stg[0-9] .. no info what this means.. so we remove it
        
        last_time = 0
        
        # parse service container
        #     container_id    | string     |       | uid of a container                          |
        # # | machine_id      | string     |       | uid of container's host machine             |
        # # | time_stamp      | bigint     |       |                                             |
        # # | app_du          | string     |       | containers with same app_du belong to same application group |
        # # | status          | string     |       |                                             |
        # # | cpu_request     | bigint     |       | 100 is 1 core                               |
        # # | cpu_limit       | bigint     |       | 100 is 1 core                               |
        # # | mem_size        | double     |       | normarlized memory, [0, 100]                |
        # # +-
        
        print(f"Start parsing service container: {service_container_file}")
        i = 0
        parsed_rows = 0
        with open(service_container_file, "r") as f:
            for row in f:
                i += 1
                if i % 500000 == 0:
                    print(".", end="", flush=True)
                # if i > 2000000:
                #     print(f"stop after {i} rows")
                #     break
                cols = list(map(str.strip, row.split(",")))
                
                assert len(cols) == 8
                try:
                    container_name = cols[0]
                    timestamp = int(cols[2])
                    if timestamp > last_time:
                        last_time = timestamp
                    domain_name = "container" + cols[3]
                    status = cols[4]
                    
                    cpu_res = int(cols[5])
                    mem_res = float(cols[7])
                    
                    if domain_name not in jobs:
                        jobs[domain_name] = Job(domain_name, "container")
                    
                    if container_name not in jobs[domain_name].task_groups:
                        jobs[domain_name].addTaskGroup(container_name, 1, cpu_res, mem_res, None, None, [])
                    
                    if status == "started" or status == "allocated":
                        if jobs[domain_name].task_groups[container_name].time_start is None:
                            jobs[domain_name].task_groups[container_name].time_start = timestamp
                        else:
                            jobs[domain_name].task_groups[container_name].time_start = min(timestamp,
                                                                                           jobs[
                                                                                               domain_name].task_groups[
                                                                                               container_name].time_start)
                    elif status == "stopped":
                        if jobs[domain_name].task_groups[container_name].time_end is None:
                            jobs[domain_name].task_groups[container_name].time_end = timestamp
                        else:
                            jobs[domain_name].task_groups[container_name].time_end = max(timestamp,
                                                                                         jobs[domain_name].task_groups[
                                                                                             container_name].time_end)
                    
                    elif status == "unknow":
                        pass  # bug in trace
                    else:
                        print(status)  # not yet implemented
                        break
                    
                    parsed_rows += 1
                except BaseException as e:
                    print(f"error while processing row: {cols}, {e}")
                    raise e
        print(f"parsed rows:{parsed_rows} out of {i}")
        
        print(f"start parsing batch: {batch_task_file}")
        # parse DAG jobs
        i = 0
        parsed_rows = 0
        with open(batch_task_file, "r") as f:
            for row in f:
                i += 1
                if i % 500000 == 0:
                    print(".", end="", flush=True)
                # if i > 10:
                #     print(f"stop after {i} rows")
                #     break
                cols = list(map(str.strip, row.split(",")))
                assert len(cols) == 9
                try:
                    task_desc = cols[0]
                    job_name = cols[2]
                    # task_type =
                    state = cols[4]
                    timestamp_start = int(cols[5])
                    assert timestamp_start >= 0
                    timestamp_end = int(cols[6])
                    assert timestamp_end >= 0
                    
                    last_time = max(last_time, timestamp_start, timestamp_end)
                    
                    if job_name not in jobs:
                        jobs[job_name] = Job(job_name, "DAG")
                    
                    # if task_id not in jobs[job_id].task_groups:
                    #     jobs[job_id].addTaskGroup(task_id, instances, res_cpu, res_mem)
                    
                    # task name is either task_RANDOM
                    # or "charNUM_NUM_NUM
                    
                    task_first_name = None
                    if task_re_independent.fullmatch(cols[0]) is None:
                        # dependent
                        # print(task_re_dependent)
                        # print(cols[0])
                        # print(re.fullmatch(r"([a-zA-Z]+)([0-9]+)(_[0-9]+)*", cols[0]))
                        
                        match = task_re_dependent.fullmatch(cols[0])
                        # task_re_dependent.
                        assert match is not None
                        groups = match.groups()
                        assert len(groups) == 4
                        task_name = groups[0] + groups[1]
                        task_first_name = groups[0]
                        if groups[2] is None:
                            dependencies = []
                        else:
                            dependencies = list(map(int, groups[2].split('_')[1:]))
                    
                    else:
                        # independent
                        task_name = cols[0]
                        dependencies = []
                    
                    if task_name not in jobs[job_name].task_groups:
                        jobs[job_name].addTaskGroup(task_name, None, None, None, timestamp_start, timestamp_end,
                                                    dependencies)
                        jobs[job_name].task_groups[task_name].first_name = task_first_name
                    
                    if timestamp_end > 0:
                        jobs[job_name].task_groups[task_name].time_end = timestamp_end
                    
                    if timestamp_start > 0:
                        jobs[job_name].task_groups[task_name].time_start = timestamp_start
                    
                    if cols[1] != "":
                        instances = int(cols[1])
                        jobs[job_name].task_groups[task_name].instances = instances
                    
                    if cols[7] != "":
                        res_cpu = float(cols[7])
                        jobs[job_name].task_groups[task_name].cpu = res_cpu
                    
                    if cols[8] != "":
                        res_mem = float(cols[8])
                        jobs[job_name].task_groups[task_name].mem = res_mem

                    
                    parsed_rows += 1
                except BaseException as e:
                    print(f"error while processing row: {cols}, {e}")
                    raise e
        
        print(f"parsed rows:{parsed_rows} out of {i}")
        
        avg_duration = []
        
        failed_j = 0
        passed_j = 0
        failed_c = 0
        passed_c = 0
        
        print("perform sanity checks")
        for j in jobs.values():
            try:
                j.parse(last_time)
                if j.job_type == "container":
                    passed_c += 1
                else:
                    passed_j += 1
                
                for tg in j.task_groups.values():
                    avg_duration.append(tg.duration)
                # if len([tg for tg in j.task_groups.values() if len(tg.dependencies) > 0]) > 0:
                
                # print(j)
                jobs_ok.append(j)
            except BaseException as e:
                # print(e)
                if j.job_type == "container":
                    failed_c += 1
                else:
                    failed_j += 1
        
        print(f"container parsing: {failed_c} failed, {passed_c} passed")
        
        print(f"job parsing: {failed_j} failed, {passed_j} passed")
        print(f"avg task duration: {sum(avg_duration) / len(avg_duration)}, max: {max(avg_duration)}")
        
        print("sort by time and write jobs")
        sep = ","
        j: Job = None
        with open(target_job_file, "w") as f:
            f.write(sep.join(["job", "type", "submit", "duration", "task_group", "instances", "mem", "cpu", "connections"]))
            k = 0
            for j in sorted(jobs_ok, key=lambda j: j.submit_time):
                
                for tg in sorted(j.task_groups.values(), key=lambda tg: tg.time_start):
                    k += 1
                    f.write("\n")
                    f.write(sep.join(map(str, [j.id, j.job_type, tg.time_start, tg.duration, tg.name, tg.instances,
                                               tg.mem, tg.cpu, '_'.join(tg.connect_to)])))
                if k > 10000:
                    f.flush()
                    k = 0
            
            f.flush()
