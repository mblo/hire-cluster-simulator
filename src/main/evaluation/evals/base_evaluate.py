import argparse
import logging
import os
import pathlib
import sys
from collections import namedtuple
from typing import Dict, List, Set

from matplotlib.backends.backend_pdf import PdfPages

experiments_directory = \
    os.path.realpath(os.path.join(pathlib.Path(__file__).parent.absolute(), "..", "..", "..", ".."))


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--experiment", "-e", action='append',
                        help="(folder) names of experiment(s) .. simply use multiple times", dest="experiment_names")

    parser.add_argument("--sweep", type=str, default=None,
                        help="sweep column", dest="sweep")

    parser.add_argument("--filter", type=str, default="",
                        help="set filter on input data. only data that passes the filter will be considered."
                             " format: key=value:key2=value2",
                        dest="filter")

    parser.add_argument("--ignore-columns", type=str, 
                        default="useSimpleTwoStateInpServerFlavorOptions:shared-resource-mode",
                        help="add columns to be ignored - i.e. those columns are removed. format: col1:col2",
                        dest="ignore_col")

    parser.add_argument("--drop-other-columns", action="store_true", default=False,
                        help="drop all other config columns.. use carefully!", dest="drop_unused_sweep_cols")

    parser.add_argument("--dry-sweeps", action="store_true", default=False,
                        help="don't plot, but dump available sweep values", dest="dry_dump_sweeps")

    parser.add_argument("--show-plots", action="store_true", default=False,
                        help="interactive mode, show plots", dest="show_plots")

    parser.add_argument("--output", "-o", type=str, default=".",
                        help="output path of generated plots, relative to experiment folder", dest="output_path")

    parser.add_argument("--cut-time", type=int, default=129600000,
                        help="absolute timestamp at which to cut of the data", dest="cut_time")

    parser.add_argument("--load-running", "-a", action="store_true", default=False,
                        help="Also consider runs that have not jet finished.", dest="load_running")

    return parser.parse_args()


EvalParams = namedtuple('EvalParams',
                        ['experiment_name',
                         'additional_experiments',
                         'output_directory',
                         'filter',
                         'sweep_column',
                         'ignore_cols',
                         'dry_run_dump_sweeps',
                         'show_plots',
                         'plots_directory',
                         "pdf",
                         "cut_time",
                         "load_running",
                         "drop_unused_sweep_cols"])


def get_evaluation_params(tmp_directory_suffix=None, name=None) -> EvalParams:
    args = parse_args()

    all_exps = args.experiment_names[:]
    if len(all_exps) > 1:
        logging.info(f"You are using the multi-experiment loader, with {len(all_exps)} exps")
    for exp in all_exps:
        if not os.path.exists(os.path.join(experiments_directory, exp)):
            logging.error(f"unknown experiment '{exp}' in root folder '{experiments_directory}'")
            sys.exit(1)

    # The directory name of the experiment to load
    experiment_name = all_exps[0]

    output_directory = os.path.realpath(
        os.path.join(os.path.realpath(experiments_directory), experiment_name, args.output_path))

    tmp_directory_name = "plots-"

    if tmp_directory_suffix is None:
        tmp_directory_name += os.path.splitext(sys.argv[0])[0]
    else:
        tmp_directory_name += tmp_directory_suffix

    plots_directory = os.path.join(output_directory, tmp_directory_name)

    logging.info(f"Processing experiment '{experiment_name}' in root directory: {experiments_directory}. "
                 f"Writing output figures to: {output_directory}.")

    # parse filter expression
    data_filter = {}
    for e in map(lambda x: x.split("="), args.filter.split(":")):
        if len(e) == 2:
            if e[0] not in data_filter:
                data_filter[e[0]] = []
            data_filter[e[0]].append(str(e[1]))

    ignore_cols: Set = set()
    for e in args.ignore_col.split(":"):
        if e not in ignore_cols:
            ignore_cols.add(str(e))

    if not os.path.exists(output_directory):
        logging.info(f"output folder does not exist, create {output_directory}")
        pathlib.Path.mkdir(pathlib.Path(output_directory), parents=True, exist_ok=True)

    if not os.path.exists(plots_directory):
        pathlib.Path.mkdir(pathlib.Path(plots_directory), parents=True, exist_ok=True)

    output_name = "plot-"

    if name is None:
        output_name += os.path.splitext(sys.argv[0])[0]
    else:
        output_name += name

    experiment_pdf = PdfPages(os.path.join(output_directory, f'{output_name}.pdf'))

    return EvalParams(experiment_name=all_exps[0],
                      additional_experiments=all_exps[1:],
                      output_directory=output_directory,
                      plots_directory=plots_directory,
                      filter=data_filter,
                      sweep_column=args.sweep,
                      dry_run_dump_sweeps=args.dry_dump_sweeps,
                      show_plots=args.show_plots,
                      pdf=experiment_pdf,
                      ignore_cols=ignore_cols,
                      cut_time=args.cut_time,
                      drop_unused_sweep_cols=args.drop_unused_sweep_cols,
                      load_running=(not args.load_running))


def finish_evaluation(params: EvalParams):
    # Save the global pdf
    params.pdf.close()

    logging.info("Finished evaluation!")


def groups_iter(use_groups, config):
    def accu(filt: Dict, remaining_groups: List):
        if len(remaining_groups) == 0:
            yield {}
        else:
            group = remaining_groups.pop()
            for val in sorted(set(config[group])):
                if len(remaining_groups) > 0:
                    for nested in accu({**filt, **{group: val}}, [r for r in remaining_groups]):
                        yield nested
                else:
                    yield {**filt, **{group: val}}

    return accu(dict(), [g for g in use_groups])


def foreach_sweep_constellation(data: dict, sweeps: list, config: dict, params: EvalParams, sink):
    from helpers.visualization_helpers import plot_text

    used_sub_selections = [k for k in sweeps if k != "scheduler" and k != params.sweep_column]
    missing_data = []

    all_env_sweeps = []
    for i, sub_select_config in enumerate(groups_iter(used_sub_selections, config)):
        # Create a string describing the current selection
        all_env_sweeps.append(f"{i + 1} -> {[k + ':' + str(sub_select_config[k]) for k in sub_select_config]}")
        # Plot the info text onto a page for information

    plot_text(
        text="All environment sweeps:\n  " + "\n  ".join(all_env_sweeps),
        params=params
    )

    for i, sub_select_config in enumerate(groups_iter(used_sub_selections, config)):

        sub_dataframes = {}

        # Initialize every dimension to the original dataframe
        for key in data:
            sub_dataframes[key] = data[key]

        for config_key in sub_select_config:
            # Apply the next restriction to the dataframe
            for key in data:
                sub_dataframes[key] = sub_dataframes[key][
                    (sub_dataframes[key][config_key] == sub_select_config[config_key])]

        # Create a string describing the current selection
        active_selection_description = f"{i + 1} -> {[k + ':' + str(sub_select_config[k]) for k in sub_select_config]}"

        # Is there any dataframe that has no more rows?
        if not [e for e in sub_dataframes if len(sub_dataframes[e]) > 0]:
            logging.info(
                f"\t{active_selection_description} [skip, rows: ({[len(sub_dataframes[k]) for k in sub_dataframes]})]")
            missing_data.append(active_selection_description)
        else:
            logging.info(
                f"\t{active_selection_description} [rows: ({[len(sub_dataframes[k]) for k in sub_dataframes]})]")

            for key in data:
                sub_dataframes[key] = sub_dataframes[key].copy()

            # Plot the info text onto a page for information
            plot_text(
                text=active_selection_description,
                params=params
            )

            # Call the sink method to operate on the data
            sink(sub_dataframes, i)

    if len(missing_data) > 0:
        plot_text(
            text="Missing data for configs:\n" + "\n".join(missing_data),
            params=params
        )
