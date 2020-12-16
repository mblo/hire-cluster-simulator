#!/usr/bin/env python3
# coding=utf-8

import gc
import logging
import os
import sys

sys.path.append(
    # Figure out the path of the evaluation directory
    os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
)

from evals.base_evaluate import experiments_directory, get_evaluation_params, finish_evaluation, EvalParams, \
    foreach_sweep_constellation
from helpers.data_table_mappers import eval_after_24h_mapper, get_eval_cut_off, \
    remove_withdrawn_jobs_from_tenant_statistics, task_group_inc_success_mapper, \
    scheduling_success_mapper, cut_all_but_last_measurements
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import plot_line_multiple, set_paper_aesthetics

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')


def str_to_safe_filename(s):
    return "".join([c for c in s if c.isalpha() or c.isdigit() or c == ' ']).rstrip()


label_x_inp = "INC demand, µ [%]"

if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-inc-success-tg",
        name="paper-inp-success-tg"
    )

    # We are interested in the cell and scheduler dumps
    types = [
        "taskgroups",
        "scheduler"
    ]

    if 'scheduler' not in params.filter:
        params.filter['scheduler'] = ["hire,parallel-0-relax-cost-ssp--simple-flavor", "hire,parallel-0-relax-cost-ssp"]

    mappers = {
        "taskgroups": [eval_after_24h_mapper,
                       remove_withdrawn_jobs_from_tenant_statistics,
                       task_group_inc_success_mapper,
                       ],
        "scheduler": [eval_after_24h_mapper, get_eval_cut_off(params), scheduling_success_mapper]
    }

    # Which columns should be kept for each type
    columns = {
        "taskgroups": ["scheduler",
                       "JobStatus",
                       "FlavorInp",
                       "TaskGroupType",
                       "ValidForJob",
                       "run",
                       "PlacementLatency_cdf_cvalue",
                       "TaskGroupsWithFlavor",
                       "TaskGroupsIncAllocated",
                       "AllocatedIncSuccess",
                       params.sweep_column],

        "scheduler": ["CountJobs", "CountJobsFinished", "CountJobsFinishedINP", "ScheduledTasks", "JobStatus",
                      "TotalJobsFullyScheduledINP", "RollingIncTaken", "RollingIncTakenRelative", "WastedAllocations",
                      "SchedulingSuccess", "AllocatedIncSuccess", "Allocations", "SimSpeed", "PastSystemMs",
                      "TotalJobsFullyScheduled", "CountJobsWithInp", "scheduler-strategy", "run",
                      "Time", "scheduler", "PendingJobsNotFullyScheduled", "PendingJobsFullyScheduled",
                      "TimesServerFlavorTaken", "TimesSwitchFlavorTaken", "TimesServerFallbackResubmit",
                      "JobsInQueue", "PendingJobsNotFullyScheduled", "PendingJobsFullyScheduled",
                      "PendingJobsNotFullyScheduledPlusFutureTgSubmissions", "CountPreemptions"]
    }

    # Load the evaluation experiment
    (data, config) = load_multiple_experiments(
        directory=experiments_directory,
        names=[params.experiment_name] + params.additional_experiments,
        types=types,
        run_data_mappers=mappers,
        run_data_columns=columns,
        filter_data=params.filter,
        ignore_cols=params.ignore_cols,
        done_required=params.load_running,
        keep_sweep_columns={params.sweep_column} if params.drop_unused_sweep_cols else {"*"},
        sweep_column=params.sweep_column)

    # set_paper_aesthetics(font_scale=2, line_width=2.5)
    fig_width = 7.5
    fig_height = 6.5
    linewidth = 2.2
    show_legend = True

    gc.collect()

    set_paper_aesthetics()


    def evaluate(dataframes: dict, i):
        data_scheduler_tg = dataframes["taskgroups"].sort_values(['run']).reset_index(drop=True)
        data_scheduler = dataframes["scheduler"]
        data_scheduler = cut_all_but_last_measurements(
            data_scheduler[data_scheduler["scheduler-strategy"] == "hire"]).sort_values(
            ['run']).reset_index(drop=True)

        data_scheduler["AllocatedIncSuccessTG"] = data_scheduler_tg["AllocatedIncSuccess"]
        data_scheduler["AllocatedIncSuccessCompleteness"] = 100.0 * (data_scheduler["AllocatedIncSuccessTG"] /
                                                                     data_scheduler["AllocatedIncSuccess"])

        data_scheduler["AllocatedIncSuccessTGNot"] = 100 - data_scheduler["AllocatedIncSuccessTG"]

        plot_line_multiple(
            styles=[
                "AllocatedIncSuccessTGNot"
            ],
            x="mu-inp",
            data=data_scheduler,
            hue="scheduler",
            x_label=label_x_inp,
            y_label="Not allocated INC\ntask groups [%]",
            ylog=False,
            ci=95,
            name=f"plot-paper-inc-tg-not-allocated-over-mu-{i}",
            params=params,
            custom_y_limit=(0, 20)
        )


    foreach_sweep_constellation(
        data=data,
        sweeps=config['sweeps'],
        config=config,
        params=params,
        sink=evaluate
    )

    finish_evaluation(
        params=params
    )
