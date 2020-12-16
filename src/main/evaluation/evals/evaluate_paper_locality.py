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
    summarize_5tuple_mapper, eval_remove_unfinished_taskgroups, SummarizeColSpecs
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import plot_line_multiple, set_paper_aesthetics

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')


def str_to_safe_filename(s):
    return "".join([c for c in s if c.isalpha() or c.isdigit() or c == ' ']).rstrip()


label_x_inp = "INC demand, µ [%]"

if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-locality",
        name="paper-locality"
    )

    types = [
        "taskgroups"
    ]

    mappers = {
        "taskgroups": [eval_after_24h_mapper,
                       remove_withdrawn_jobs_from_tenant_statistics,
                       eval_remove_unfinished_taskgroups(params),
                       summarize_5tuple_mapper(params,
                                               [
                                                   SummarizeColSpecs(column='DetourInc', agg_columns=[],
                                                                     filt=lambda x: x[x['DetourInc'] >= 0]),
                                                   SummarizeColSpecs(column='MaxDiameter',
                                                                     agg_columns=["TaskGroupType"],
                                                                     filt=lambda x: x[x['MaxDiameter'] >= 0]),
                                                   SummarizeColSpecs(column='InvolvedRacks',
                                                                     agg_columns=["TaskGroupType"],
                                                                     filt=lambda x: x[x['InvolvedRacks'] >= 0]),
                                               ]),

                       ],
    }

    # Which columns should be kept for each type
    columns = {
        "taskgroups": ["scheduler",
                       "JobStatus",
                       "TaskGroupID",
                       "Duration",
                       "run",
                       "SubmissionTime",
                       "TotalTasks",
                       "TaskGroupType",
                       "TasksStarted",
                       "MaxDiameter",
                       "MaxDiameter_mean",
                       "MaxDiameter_median",
                       "MaxDiameter_percentil25",
                       "MaxDiameter_percentil95",
                       "MaxDiameter_percentil99",
                       "DetourInc",
                       "DetourInc_mean",
                       "DetourInc_median",
                       "DetourInc_percentil25",
                       "DetourInc_percentil95",
                       "DetourInc_percentil99",
                       "InvolvedRacks",
                       "InvolvedRacks_mean",
                       "InvolvedRacks_median",
                       "InvolvedRacks_percentil25",
                       "InvolvedRacks_percentil95",
                       "InvolvedRacks_percentil99",
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

        for tmp in [0.05, 0.5, 1.0]:
            print(f"INP:{tmp}")
            for tmp_sched in df["scheduler"].unique():
                print(f"     scheduler:{tmp_sched}")
                print(df[(df["scheduler"] == tmp_sched) &
                         (df["mu-inp"] == tmp)][["DetourInc_mean"]].describe())

        plot_line_multiple(
            styles=[
                "DetourInc_mean"
            ],
            x="mu-inp",
            data=df,
            hue="scheduler",
            x_label=label_x_inp,
            y_label=f"Switch detours",
            ylog=False,
            ci=99,
            name=f"plot-paper-locality-detours",
            params=params,
            custom_y_limit=(0, 2)
        )

        for metric in ["DetourInc_mean",
                       "DetourInc_median",
                       "DetourInc_percentil25",
                       "DetourInc_percentil95",
                       "DetourInc_percentil99",
                       "MaxDiameter_mean",
                       "MaxDiameter_median",
                       "MaxDiameter_percentil25",
                       "MaxDiameter_percentil95",
                       "MaxDiameter_percentil99",
                       "InvolvedRacks_mean",
                       "InvolvedRacks_median",
                       "InvolvedRacks_percentil25",
                       "InvolvedRacks_percentil95",
                       "InvolvedRacks_percentil99"
                       ]:
            plot_line_multiple(
                styles=[
                    metric
                ],
                x="mu-inp",
                data=df,
                hue="scheduler",
                x_label=label_x_inp,
                y_label=f"{metric}",
                ylog=False,
                name=f"plot-paper-locality-metric-summary-{metric}",
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
