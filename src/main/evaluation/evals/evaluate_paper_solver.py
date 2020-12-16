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
from helpers.data_table_mappers import solver_time_mapper, summarize_cdf
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import set_paper_aesthetics, plot_rel_multiple, plot_line_multiple

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')


def str_to_safe_filename(s):
    return "".join([c for c in s if c.isalpha() or c.isdigit() or c == ' ']).rstrip()


if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-solver",
        name="paper-solver"
    )

    # We are interested in the cell and scheduler dumps
    types = [
        "solver"
    ]

    mappers = {
        "solver": [solver_time_mapper,
                   summarize_cdf(params, 'ElapsedTimeMs')]
    }

    if 'scheduler' not in params.filter:
        params.filter['scheduler'] = ["hire,parallel-0-relax-cost-ssp"]

    # Which columns should be kept for each type
    columns = {
        "solver": ["ElapsedTimeMs",
                   "ElapsedTimeMs_cdf_bucket",
                   "ElapsedTimeMs_cdf_value",
                   "ElapsedTimeMs_cdf_cvalue",
                   "Supply", "Solver", "scheduler", "EndTimeNanos", "StartTimeNanos", "mu-inp",
                   "NonProducerCnt", "ParallelRun", "ProducerCnt",
                   ]
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

    fig_width = 7.5
    fig_height = 6.5
    linewidth = 2.2
    show_legend = True

    gc.collect()

    set_paper_aesthetics()


    def evaluate(dataframes: dict, i):
        df = dataframes["solver"]

        df["µ"] = df["mu-inp"]

        plot_rel_multiple(
            x="ElapsedTimeMs_cdf_bucket",
            y="ElapsedTimeMs_cdf_value",
            x_label=f'Solver time ' + r' [ms; $log_{10}$]',
            y_label=r"Probability ($\leq x$)",
            hue="µ",
            data=df,
            name=f"-cdf",
            params=params,
            log_y=False,
            reverse_hue_order=False,
            custom_size_aspect=1.35
        )

        plot_rel_multiple(
            x="ElapsedTimeMs_cdf_bucket",
            y="ElapsedTimeMs_cdf_cvalue",
            x_label=f'Solver time ' + r' [ms; $log_{10}$]',
            y_label=r"Probability ($> x$)",
            hue="µ",
            data=df,
            name=f"--ccdf",
            params=params,
            reverse_hue_order=False,
            custom_size_aspect=1.35
        )

        df["µ"] = df["µ"].apply(lambda tmp: f"µ={tmp}")

        plot_line_multiple(
            styles=[
                "ElapsedTimeMs_cdf_value"
            ],
            x="ElapsedTimeMs_cdf_bucket",
            data=df,
            hue="µ",
            x_label="Solver time",
            y_label="CDF",
            this_hue_order=["µ=0.0", "µ=0.5", "µ=1.0"],
            ylog=True,
            ci=95,
            name=f"plot-broken-cdf-{i}",
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
