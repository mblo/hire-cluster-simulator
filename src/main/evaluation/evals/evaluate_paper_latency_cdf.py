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
from helpers.data_table_mappers import eval_after_24h_mapper, remove_withdrawn_jobs_from_tenant_statistics, \
    eval_tasks_not_started_mapper, \
    summarize_cdf
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import set_paper_aesthetics, plot_rel_multiple

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')


def str_to_safe_filename(s):
    return "".join([c for c in s if c.isalpha() or c.isdigit() or c == ' ']).rstrip()


label_x_inp = "Jobs with INC, µ [%]"

if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-latency-cdf",
        name="paper-latency-cdf"
    )

    if 'mu-inp' not in params.filter:
        logging.warning("strictly enforce filter of mu-inp=1.0")
        params.filter['mu-inp'] = ["1.0"]

    # We are interested in the cell and scheduler dumps
    types = [
        "taskgroups"
    ]

    mappers = {
        "taskgroups": [eval_after_24h_mapper,
                       remove_withdrawn_jobs_from_tenant_statistics,
                       eval_tasks_not_started_mapper(params),
                       summarize_cdf(params, 'PlacementLatency'),
                       ],
    }

    # Which columns should be kept for each type
    columns = {
        "taskgroups": ["scheduler",
                       "JobStatus",
                       "TaskGroupID",
                       "TaskGroupType",
                       "ValidForJob",
                       "Duration",
                       "run",
                       "SubmissionTime",
                       "TotalTasks",
                       "TasksStarted",
                       "PlacementLatency",
                       "PlacementLatency_cdf_bucket",
                       "PlacementLatency_cdf_value",
                       "PlacementLatency_cdf_cvalue",
                       params.sweep_column]
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
        df = dataframes["taskgroups"]

        for mu in df["mu-inp"].unique():
            plot_rel_multiple(
                x="PlacementLatency_cdf_bucket",
                y="PlacementLatency_cdf_cvalue",
                x_label=f'Placement latency ' + r' [ms; $log_{10}$]',
                y_label=r"Probability ($> x$)",
                hue="scheduler",
                data=df[df["mu-inp"] == mu],
                name=f"latency-{mu}-ccdf",
                params=params,
                reverse_hue_order=True
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
