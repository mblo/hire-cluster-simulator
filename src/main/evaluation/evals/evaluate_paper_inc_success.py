#!/usr/bin/env python3
# coding=utf-8

import logging
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
)

from evals.base_evaluate import experiments_directory, get_evaluation_params, finish_evaluation, EvalParams, \
    foreach_sweep_constellation
from helpers.data_table_mappers import eval_after_24h_mapper, get_eval_cut_off, scheduling_success_mapper, \
    cut_all_but_last_measurements
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import plot_line_multiple, set_paper_aesthetics

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')

label_x_inp = "INC demand, µ [%]"

if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-inc-success",
        name="paper-inp-success"
    )

    # We are interested in the cell and scheduler dumps
    types = [
        "scheduler"
    ]

    mappers = {
        # Add a basic mapper that drops every row that belongs to a timestamp before the 24h mark
        "scheduler": [eval_after_24h_mapper, get_eval_cut_off(params), scheduling_success_mapper]
    }

    columns = {
        "scheduler": ["CountJobs", "CountJobsFinished", "CountJobsFinishedINP", "ScheduledTasks", "JobStatus",
                      "TotalJobsFullyScheduledINP", "RollingIncTaken", "RollingIncTakenRelative", "WastedAllocations",
                      "SchedulingSuccess", "AllocatedIncSuccess", "Allocations", "SimSpeed", "PastSystemMs",
                      "TotalJobsFullyScheduled", "CountJobsWithInp",
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

    set_paper_aesthetics()


    def evaluate(dataframes: dict, i):
        data_scheduler = dataframes["scheduler"]

        d = cut_all_but_last_measurements(data_scheduler)

        plot_line_multiple(
            styles=["AllocatedIncSuccess"],
            x="mu-inp",
            data=d,
            hue="scheduler",
            x_label=label_x_inp,
            y_label="Satisfied INC jobs [%]",
            ylog=False,
            ci=99,
            name=f"plot-paper-inc-success-over-mu-{i}",
            params=params,
        )

        plot_line_multiple(
            styles=["WastedAllocations"],
            x="mu-inp",
            data=d,
            hue="scheduler",
            x_label=label_x_inp,
            y_label="Preempted tasks [%]",
            ylog=False,
            ci=99,
            name=f"plot-paper-preemptions-over-mu-{i}",
            params=params
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
