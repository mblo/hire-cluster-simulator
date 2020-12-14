#!/usr/bin/env python3
# coding=utf-8

import argparse
import copy
import curses
import datetime
import itertools
import json
import math
import os
import queue
import subprocess
import sys
import threading
import time
from typing import Dict
from zipfile import ZipFile, ZIP_STORED

assert (sys.version_info >= (3, 7, 0))

CHECK_PROCESS_EACH = 30

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--output", type=str, help="output directory of all experiments", required=True)
    parser.add_argument("--worker", type=int, help="number of JVMs allowed to run in parallel", required=False,
                        default=1)
    parser.add_argument("--memory", type=int, help="max memory a jvm could take", default=30)
    parser.add_argument("--jar", type=str, default="target/hire.jar")
    parser.add_argument("--dry", action="store_true", default=False)
    parser.add_argument("--set", nargs='*', type=str, dest="set_config",
                        help="set as many config parameters as desired, e.g. "
                             "--set seed=3:5:1 scheduler=hire,parallel-0-relax-cost-ssp ...")

    args = parser.parse_args()

    # prepare cartesian product
    custom_configs = []
    for cc in args.set_config:
        keys = cc.split('=')
        assert len(keys) == 2, f"invalid argument: {keys}"
        values = keys[1].split(':')
        values = [True if v in ["True", "true"] else False if v in ["False", "false"] else v for v in values]
        keys = f"--{keys[0]}"
        custom_configs.append([(keys, v) for v in values])

    output_root_dir = os.path.abspath(args.output)
    if not args.dry:
        os.makedirs(output_root_dir, exist_ok=True)

    if not os.path.exists(args.jar) and not args.dry:
        raise NameError(f"cannot find jar at:{args.jar}")

    if not (1 < args.memory < 300):
        raise NameError(f"seems like you have a wrong jvm memory config")

    default_config = {
        '--id': -1,
        '--scheduler': 'hire,parallel-0-relax-cost-ssp',
        '--seed': 42,
        '--mu-inp': 1.0,
        '--ratioOfIncTaskGroups': 0.25,
        '--cell-max-active-inp': 3,
        '--create-inp-starting-time': -1,
        '--skip-jobs-before-time': -1,
        '--cellSwitchHomogeneous': 'homogeneous',  # homogeneous or random2
        '--hireInpServerPenaltyCost': 3.0,
        '--hireInpServerPenaltyWaitingLower': 10000,
        '--hireInpServerPenaltyWaitingUpper': 100000,
        '--hireShortcutsMaxSearchSpace': 200,
        '--hireShortcutsMaxSelection': 50,
        '--sim-time': 5 * 3600 * 1000,
        '--precision': 100,
        '--maxServerPressure': .98,
        '--softLimitProducersInGraph': 200,
        '--softLimitSupplyInGraph': -1,
        '--flowSchedulerPostponeSchedulingIfStuck': 1000,
        '--sspMaxSearchTimeSeconds': 300,
        '--minQueuingTimeBeforePreemption': 8000,
        '--maxInpFlavorDecisionsPerRound': 100,
        '--celltype': 'ft',
        '--cell-k': 26,
        '--shared-resource-mode': "deactivated",
        '--scale-cell-switches': '1.0,1.0,1.0',
        '--scale-cell-servers': '1.0,1.0',
        '--max-switch-capacity': 1000000,
        '--max-server-capacity': 1000000,
        '--kappa-tasks': 0.1,
        '--kappa-runtime': 0.1,
        '--kappa-draw-random': True,
        '--workload-time': -1,
        '--useSimpleTwoStateInpServerFlavorOptions': False,
        '--tgMaxDuration': -1,
        '--inp-types': 'netchain',
        '--statistics-tenant-extra-snapshots-hours': "/",
        '--statistics-tenant-job': True,
        '--statistics-tenant-tg': True,
        '--statistics-scheduler': True,
        '--statistics-cell': True,
        '--statistics-inp-cell': True,
        '--statistics-solver': True,
        '--statistics-timing': True,
        '--statistics-interval': 60000,
        '--status-report-message': 300,
        '--statistics-start': 3600000,
        '--disableLimits': 0,  # Disable the backlog after
        '--think-time-scaling': 1.0,
        '--verbose': 0,
        '--time-it': False
    }

    pending_jobs: queue.Queue = queue.Queue()

    worker_mutex_start_new = threading.RLock()


    def new_config_for_run():
        if "i" not in new_config_for_run.__dict__:
            new_config_for_run.i = -1

        new_config_for_run.i += 1

        tmp = copy.deepcopy(default_config)
        tmp['--id'] = new_config_for_run.i

        if not args.dry:
            target_folder = os.path.join(output_root_dir, f"run-{new_config_for_run.i}")
            if os.path.exists(target_folder):
                raise NameError(f"target folder of experiment already exists: {target_folder}")
            os.makedirs(target_folder)
            tmp['--output-dir'] = target_folder
        return tmp


    very_first_config = None


    def enqueue_run(config):
        global very_first_config

        if very_first_config is None:
            very_first_config = copy.deepcopy(config)
        pending_jobs.put_nowait(config)
        if not args.dry:
            with open(os.path.join(config['--output-dir'], "config.json"), "w") as f:
                json.dump(config, f)


    for x in itertools.product(*custom_configs):
        config = new_config_for_run()
        for k, v in x:
            assert k in config, f"missing key {k} in config, seems an invalid config parameter"
            config[k] = v

        print(f"prepare config with: {x}")

        # don't do useless experiments..
        skip = False

        inp_scheduler_strategies = [",force-inp", ",resubmit", ",fallback"]
        for strategy in inp_scheduler_strategies:
            # skip inp strategies when we are in server mode, however, allow for CoCo (coco always runs INP strategy)
            if strategy in config['--scheduler'] and float(config['--mu-inp']) == 0.0 and "coco" not in config[
                '--scheduler']:
                skip = True

        if "--hireInpServerPenaltyWaitingLower" in config and "--hireInpServerPenaltyWaitingUpper" in config:
            if float(config["--hireInpServerPenaltyWaitingLower"]) >= float(
                    config["--hireInpServerPenaltyWaitingUpper"]):
                skip = True

        # hire should not to run with "foreach" shared-resource-mode
        if "hire" in config["--scheduler"] and config["--shared-resource-mode"] == "foreach":
            skip = True
        # all other schedulers should not run with "once" shared-resource-mode
        elif "hire" not in config["--scheduler"] and config["--shared-resource-mode"] == "once":
            skip = True

        # only hire has complex INP flavor mode
        if "hire" not in config["--scheduler"] and not config["--useSimpleTwoStateInpServerFlavorOptions"]:
            skip = True

        server_scheduler_strategies = [",force-server"]
        for strategy in server_scheduler_strategies:
            if strategy in config['--scheduler'] and float(config['--mu-inp']) > 0.0:
                skip = True

        sweep_keys_for_inp_workload = [
            "--cell-max-active-inp",
            "--scale-cell-switches",
            "--kappa-tasks",
            '--maxInpFlavorDecisionsPerRound',
            "--kappa-runtime",
            "--cellSwitchHomogeneous"]

        hire_specific_config_keys = [
            '--precision',
            '--softLimitProducersInGraph',
            '--softLimitSupplyInGraph',
            '--hireShortcutsMaxSearchSpace',
            '--hireShortcutsMaxSelection',
            '--useSimpleTwoStateInpServerFlavorOptions',
            '--sspMaxSearchTimeSeconds',
            '--minQueuingTimeBeforePreemption',
            '--maxInpFlavorDecisionsPerRound',
            '--postponeScheduleTimeWindow']

        if very_first_config is not None:
            # check for the inp related keys.. so that we ignore sweeps when mu is 0
            if float(config['--mu-inp']) == 0.0:
                for check_key in sweep_keys_for_inp_workload:
                    if config[check_key] != very_first_config[check_key]:
                        skip = True

            # ignore hire specific config values, when we run a different scheduler than hire..
            if "hire" not in config['--scheduler']:
                for check_key in hire_specific_config_keys:
                    if "hire" in check_key and config[check_key] != very_first_config[check_key]:
                        skip = True

        if skip:
            print(f"\t###### skip useless configuration")
            continue

        enqueue_run(config)

    print(f"### in total (appx) {pending_jobs.qsize()} jobs")

    initial_job_size = pending_jobs.qsize()
    if initial_job_size == 0:
        print("There is no job in the queue!")

    finished_runs = 0
    ongoing_runs = 0

    if args.dry:
        print("--- DRY RUN")
        from terminaltables import AsciiTable

        # get all valid configs
        all_configs = {}
        largest_config_id = 0
        while not pending_jobs.empty():
            item = pending_jobs.get_nowait()
            assert (item["--id"] not in all_configs)
            print(f"   exp with id:{item['--id']}")
            all_configs[item["--id"]] = item
            if largest_config_id < item["--id"]:
                largest_config_id = item["--id"]


        def diff_to_default(config):
            tmp = {}
            for k in config:
                if k in very_first_config and config[k] != very_first_config[k]:
                    # print(f"different values for {k}: {config[k]} != {very_first_config[k]}")
                    tmp[k] = copy.copy(config[k])
                elif k == "--id":
                    tmp[k] = copy.copy(config[k])
            return tmp


        def shorten_text(t):
            t = str(t)
            MAX_LEN = 15
            if len(t) < MAX_LEN:
                return t
            chunks = math.ceil(len(t) / MAX_LEN)

            return "\n".join([t[c * MAX_LEN:(c + 1) * MAX_LEN] for c in range(chunks)])


        def ask_for_user_input(stdscr):
            # c 99 up 259 down 258
            man = f"howto: up/down for run id ; 'c' clears table, 'p' prints cmd ## {len(all_configs)} configs "
            stdscr.addstr(man + ' ')
            stdscr.refresh()
            stdscr.move(1, 0)
            run_id = 0
            sweeps = []
            keys = ['--id']
            max_size = stdscr.getmaxyx()
            error = ""
            if len(all_configs) == 0:
                raise NameError("there are no configs available")
            while True:
                params = diff_to_default(all_configs[run_id]) if run_id != 0 else all_configs[run_id]
                assert (params['--id'] == run_id)

                sweeps.append(params)
                # update columns?
                for k in params:
                    if k not in keys:
                        keys.append(k)

                table = AsciiTable([['\n'.join(k.split('_')) for k in keys]] +
                                   [[shorten_text(run[k]) if k in run else '/' for k in keys] for run in
                                    sweeps]).table

                row = 1
                for l in table.splitlines():
                    if row >= max_size[0] - 3:
                        error = "too many lines"
                        break
                    stdscr.move(row, 0)
                    row += 1
                    stdscr.addstr(l[:max_size[1]] + ' ')

                if len(error) > 0:
                    stdscr.move(0, len(man))
                    if error == " ":
                        stdscr.addstr(" " * 40)
                        error = ""
                    else:
                        stdscr.addstr("####  ERROR: " + error + ' ####')
                    error = " "

                stdscr.refresh()

                c = stdscr.getch()
                if c == 99:
                    sweeps = []
                    keys = ['--id']
                    stdscr.move(1, 0)
                    stdscr.clrtobot()

                while True:
                    if c == 259:
                        run_id += 1
                    elif c == 258:
                        run_id -= 1
                    elif c == 339:
                        run_id *= 2
                        c = 259
                    elif c == 338:
                        run_id = run_id // 2
                        c = 258
                    elif c == 99:
                        break
                    elif c == 112:
                        # print current config
                        stdscr.move(1, 0)
                        stdscr.addstr("\ncmd for run:" + str(run_id) + "\n")
                        stdscr.addstr(" ".join(
                            [f"{x} {all_configs[run_id][x]}" if all_configs[run_id][x] is not None else f"{x}" for x in
                             all_configs[run_id]]))
                        stdscr.addstr("\n\n\nPress any key to refresh table...\n\n\n")
                        c = stdscr.getch()
                        continue

                    else:
                        error = f"Unknown Keypress ({c})"
                        break

                    if run_id < 0 or run_id > largest_config_id:
                        run_id = 0

                    if run_id in all_configs:
                        break

                stdscr.move(1, 0)


        curses.wrapper(ask_for_user_input)
        sys.exit(0)


    def start_and_observe_jvm(config: Dict):
        sim_args = []
        for k, v in config.items():
            sim_args.append(str(k))
            if v is not None:
                sim_args.append(str(v))

        start_time = time.time()
        running = True
        failed_checks = 0
        current_target = config['--output-dir']
        process = None

        with open(os.path.join(current_target, "cmd"), "w") as f:
            f.write(" ".join(['java', f"-Xmx{args.memory}G",
                              '-jar', args.jar] + sim_args))

        try:
            print(f"Start {current_target} at {datetime.datetime.now()}")
            with open(os.path.join(current_target, "out.txt"), "wb") as out, open(
                    os.path.join(current_target, "err.txt"), "wb") as err:
                process = subprocess.Popen(['java', f"-Xmx{args.memory}G",
                                            '-jar', args.jar] + sim_args,
                                           close_fds=False,
                                           stdout=out, stderr=err)
                with open(os.path.join(current_target, "pid"), "w") as f:
                    f.write(str(process.pid))

                while running:
                    time.sleep(CHECK_PROCESS_EACH)
                    try:
                        if process.poll() is not None:
                            running = False
                        failed_checks = 0
                    except BaseException as e:
                        failed_checks += 1
                        print(f".. failed to get status of {current_target}")
                        if failed_checks > 100:
                            print(f"a worker died... {current_target}")
                            raise e
                    # still running

                ret = process.returncode
                if ret != 0:
                    print(f"A worker is finished, but returned unexpected signal:{ret}, worker:{current_target}")

                print(f"Worker done after {round((time.time() - start_time) / 3600, 2)} hours; {current_target}")

            print(f"apply compression of {current_target}")
            compression_files = ["taskgroups.csv", "solver.csv", "out.txt", "jobs.csv", "timings.csv", "cell.csv",
                                 "cell-inp.csv", "scheduler.csv"]
            compression_files = [(x, os.path.join(current_target, x)) for x in compression_files]
            with ZipFile(os.path.join(current_target, "stats.zip"), mode='w', compression=ZIP_STORED) as myzip:
                for compress_file in compression_files:
                    if os.path.isfile(compress_file[1]):
                        myzip.write(compress_file[1], compress_file[0])
                assert (myzip.testzip() is None)

            for compress_file in compression_files:
                if os.path.isfile(compress_file[1]):
                    os.remove(compress_file[1])
            print(f" ..  compression of {current_target} done")

        except BaseException as e:
            print(f"A worker totally failed, {current_target}, {e}, (worker: {current_target})")

        finally:
            try:
                process.kill()
            except BaseException as e2:
                print(f" .. exception in killing process: {e2}")


    previous_progress_pending_report = 1.0
    previous_progress_done_report = 0
    no_work_left = False


    def worker():
        global previous_progress_pending_report, finished_runs, no_work_left, previous_progress_done_report, ongoing_runs
        while True:
            item = pending_jobs.get()
            if item is None:
                break
            ongoing_runs += 1
            try:
                start_and_observe_jvm(item)
            except BaseException as e:
                print(f"worker failed in outer look, {e}")
            pending_jobs.task_done()
            finished_runs += 1
            ongoing_runs -= 1
            progress_pending = pending_jobs.qsize() / initial_job_size
            progress_done = finished_runs / initial_job_size
            if (no_work_left != pending_jobs.empty()
                    or previous_progress_pending_report - progress_pending > 0.2
                    or progress_done - previous_progress_done_report > 0.1):
                previous_progress_pending_report = progress_pending
                previous_progress_done_report = progress_done
                no_work_left = pending_jobs.empty()


    threads = []
    workers = args.worker
    for i in range(workers):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    try:
        pending_jobs.join()
        print("all work seems to be done")

    except KeyboardInterrupt:
        print(f"You pressed crtl-c ... going to shutdown everything")

        while pending_jobs.qsize() > 0:
            try:
                pending_jobs.get_nowait()
            except BaseException:
                break

    # stop workers
    for i in range(workers):
        pending_jobs.put(None)
    for t in threads:
        t.join()
