import logging
import os
import sys
from typing import Dict, Set, List
from zipfile import ZipFile

from helpers.statistics_globals import int_columns, float_columns, bool_columns

assert sys.version_info >= (3, 7)
import pandas as pd  # version >= 0.23 requiered
import json
import gc
from glob import glob
from os import stat
from os.path import exists, join


excluded_sweeps = ["run", "experiment", "scheduler-strategy"]


def load_single_experiment(directory, name, types,
                           run_data_mappers: dict = None, run_data_columns=None,
                           done_required=True, filter_data: Dict = None,
                           keep_sweep_columns: Set[str] = None) -> tuple:
    return load_multiple_experiments(
        directory=directory,
        names=[name],
        types=types,
        filter_data=filter_data,
        run_data_mappers=run_data_mappers,
        run_data_columns=run_data_columns,
        done_required=done_required,
        keep_sweep_columns=keep_sweep_columns)


def load_multiple_experiments(directory, names, types,
                              run_data_mappers: dict = None, run_data_columns=None,
                              done_required=True, filter_data: Dict = None,
                              keep_sweep_columns: Set[str] = None,
                              ignore_cols: Set[str] = None,
                              drop_seed=False,
                              data_columns_after_postprocessing: Dict[str, List[str]] = None,
                              sweep_column: str = None) -> tuple:
    # Make sure there is at least one dimension to analyze
    assert types

    # Counter for uniquely identifying experiments
    experiment_counter = 0

    # The global config containing all possible assignments for any parameter
    global_config = {}
    # The local configs
    configs = []

    for name in names:
        # The path to the directory the experiment is located in
        experiment_directory = join(directory, name)

        # And load the experiments configs
        load_experiment_configs(
            global_config=global_config,
            experiment=experiment_counter,
            experiment_directory=experiment_directory,
            done_required=done_required,
            filter_data=filter_data,
            configs=configs,
            ignore_cols=ignore_cols,
            drop_seed=drop_seed
        )

        # Advance experiment counter
        experiment_counter += 1

    # The amount of runs to load is just the amount of configs we got
    total_runs = len(configs)

    # Find the config keys with multiple sweeps
    sweeps = [key for key in global_config.keys() if
              len(global_config[key]) > 1 or key == "scheduler" or key == "scheduler-strategy"]
    kept_sweeps = sweeps[:]

    # If wanted we can keep the sweep columns
    if keep_sweep_columns is not None:
        kept_sweeps = []

        # consider cases when our sweep column has only one value present
        if sweep_column is not None and sweep_column not in keep_sweep_columns:
            sweeps.append(sweep_column)

        for sweep in sweeps:
            if sweep in keep_sweep_columns or "*" in keep_sweep_columns:
                if sweep != "seed":
                    for type in types:
                        if sweep not in run_data_columns[type]:
                            run_data_columns[type].append(sweep)

                            if "mu-inp" not in sweeps and "mu-inp" not in run_data_columns[type]:
                                run_data_columns[type].append("mu-inp")

                            if sweep not in kept_sweeps:
                                kept_sweeps.append(sweep)

    # The global accessible sweep list has an exclusion list (e.g. for scheduler)
    kept_sweeps = [k for k in kept_sweeps if k not in excluded_sweeps]

    global_config["sweeps"] = [key for key in kept_sweeps if key not in excluded_sweeps]

    print(f"sweeps: {sweeps}")
    print(f"kept_sweeps: {kept_sweeps}")
    print(f"global sweeps: {global_config['sweeps']}")
    for k in global_config:
        if len(global_config[k]) > 1 and k not in excluded_sweeps:
            print(f"\t{k}: {global_config[k]}")

    # Give some info about we are now doing
    logging.info(
        f"Found {total_runs}. Configuration parameters with multiple assignments are: {', '.join(k for k in kept_sweeps)}. Now loading data...")
    del sweeps

    # Containing the datatables by type name
    results = {}

    # Fill datatable dict with empty lists
    for type in types:
        results[type] = []

    # Now load the datatables for the configs
    for i, config in enumerate(configs):
        # Each config entry represents one run to be loaded
        logging.info(f"Status {i}/{total_runs}")
        load_run(
            config_entry=config,
            results=results,
            types=types,
            mappers=run_data_mappers,
            columns=run_data_columns,
            data_columns_after_postprocessing=data_columns_after_postprocessing
        )
        gc.collect()

    # Unify each list into a table for later processing
    for type in types:
        results[type] = pd.concat(results[type], copy=False)

    # Ask the gc to cleanup some garbage we produced along the way
    gc.collect()
    # Notify completion
    logging.info("Completed loading process!")

    return results, global_config


def load_run(config_entry: Dict, results, types, mappers, columns,
             data_columns_after_postprocessing: Dict[str, List[str]] = None):
    run_directory = config_entry["directory"]
    run_config = config_entry["config"]

    # Give some info about this run
    logging.info(f"Loading datatables for run {run_directory}...")

    for type in types:
        # either read csv directly, or using the zip file
        target_csv = f"{type}.csv"
        target_zip = f"stats.zip"

        def process(file):
            # Get the mapper if there is any for that type
            target_mappers = mappers.get(type) if mappers is not None else None
            # Get the columns to keep if there are any for that type. If None all will be kept
            target_columns = columns.get(type) if columns is not None else None

            target_columns_after_postprocessing = data_columns_after_postprocessing.get(
                type) if data_columns_after_postprocessing is not None else None

            # First load the data for that type
            data = load_data_table(
                file=file,
                config=run_config,
                mappers=target_mappers,
                columns=target_columns,
                data_columns_after_postprocessing=target_columns_after_postprocessing)

            # Then append to the correct list
            results[type].append(data)

        file = join(run_directory, target_csv)
        if os.path.isfile(file):
            if stat(file).st_size == 0:
                logging.info(f"Experiment {run_directory} has no data for {type} (csv empty) ... Skipping")
            else:
                process(file)
        else:
            file = join(run_directory, target_zip)
            if os.path.isfile(file):
                with ZipFile(file) as myzip:
                    with myzip.open(target_csv) as myfile:
                        process(myfile)
            else:
                logging.info(f"Experiment {run_directory} has no data for {type} (no zip found) ... Skipping")


def load_data_table(file, config, mappers=None, columns=None,
                    data_columns_after_postprocessing: Set[str] = None):
    cell = pd.read_csv(file, index_col=None)

    # Swap in configuration data
    for key in config:
        assert (key not in cell.keys()), f"{key} found in cell keys"
        if columns is None or key in columns:
            cell[key] = config[key]

    # Remove columns that are not flagged to be kept
    if columns is not None:
        # All columns that should be removed
        to_remove = [c for c in cell.keys() if c not in columns]
        # Only remove if there is anything to remove
        if len(to_remove) > 0:
            # Execute the removal
            cell.drop(to_remove, axis=1, inplace=True)

    # Map columns to corresponding types
    for key in cell.keys():
        if key in int_columns:
            cell[key] = cell[key].astype('int64')
        elif key in float_columns:
            cell[key] = cell[key].astype('float64')
        else:
            logging.debug(f"unknown key data type: {key}")

    if "Time" in cell.columns:
        cell.set_index('Time', drop=False)
    elif "SubmissionTime" in cell.columns:
        cell.set_index('SubmissionTime', drop=False)

    # df[floats].apply(pd.to_numeric, downcast='float')

    # Execute mappers if any (pre removal of columns)
    if mappers is not None:
        for mapper in mappers:
            cell = mapper(cell)

    # check if there is a list of keep-columns after postprocessing
    if data_columns_after_postprocessing is not None:
        # All columns that should be removed
        to_remove = [c for c in cell.keys() if c not in data_columns_after_postprocessing and c != "scheduler-strategy"]
        # Only remove if there is anything to remove
        if len(to_remove) > 0:
            # Execute the removal
            cell.drop(to_remove, axis=1, inplace=True)

    return cell


def load_experiment_configs(global_config, configs: list, experiment_directory,
                            experiment, done_required=True, filter_data: Dict = None, drop_seed=False,
                            ignore_cols: Set[str] = None):
    # Set the run counter to 0
    run_counter = 0

    for run_directory in glob(join(experiment_directory, "run-[0-9]*")):

        started = exists(join(run_directory, "START"))
        completed = exists(join(run_directory, "DONE"))

        if not started:
            logging.info(f"Experiment {run_directory} has not started... Skipping")
        elif not completed and done_required:
            logging.info(f"Experiment {run_directory} has not completed... Skipping")
        else:
            logging.info(f"Loading experiment {run_directory}...")

            # Load the config for this run
            config = load_config(
                global_config=global_config,
                experiment=experiment,
                run=run_counter,
                directory=run_directory,
                filter_data=filter_data,
                drop_seed=drop_seed,
                ignore_cols=ignore_cols
            )

            # If the load was successful add a entry (might fail when there is a filter)
            if config is not None:
                # Add a entry to the configs list
                configs.append({
                    'directory': run_directory,
                    'config': config
                })
                # Increment such that the next run has a different id
                run_counter += 1
            else:
                logging.info(f"Experiment {run_directory} filtered... Skipping")


def load_config(global_config, directory, experiment, run, filter_data: Dict = None, drop_seed=False,
                ignore_cols: Set[str] = None):
    with open(join(directory, "config.json")) as config_file:

        # Load the configuration of that experiment
        config = json.load(config_file)
        # Set its unique id's accordingly
        config["experiment"] = experiment

        for key in [tbd for tbd in config.keys() if tbd.startswith("--")]:
            value = config[key]
            config[key[2:]] = value
            del config[key]

        for key in config.keys():
            if key in int_columns:
                config[key] = int(config[key])
            if key in float_columns:
                config[key] = float(config[key])
            if key in bool_columns:
                config[key] = (config[key] is True or config[key] == "true")

        config["run"] = config["id"]

        # Remove useless keys
        for key in ["id", "output-dir"]:
            if key in config:
                del config[key]

        if drop_seed and "seed" in config:
            del config["seed"]

        # extract scheduler family
        sched_strategy = "unknown"
        if 'scheduler' in config:
            sched_strategy_candidates = ["hire", "resubmit", "greedy"]

            for sched_strategy_check in sched_strategy_candidates:
                if sched_strategy_check in config["scheduler"].lower():
                    sched_strategy = sched_strategy_check
                    break

            if "useSimpleTwoStateInpServerFlavorOptions" not in config:
                logging.warning("useSimpleTwoStateInpServerFlavorOptions missing in config.. use default")
                config["useSimpleTwoStateInpServerFlavorOptions"] = True

            if (config['scheduler'].startswith("hire,")
                    and config["useSimpleTwoStateInpServerFlavorOptions"] == True):
                logging.info("      -> detect HIRE with simple-flavor")
                config['scheduler'] = f"{config['scheduler']}--simple-flavor"

        config["scheduler-strategy"] = sched_strategy

        # Check the filter if we have any
        if filter_data is not None:
            for k in filter_data:
                if k not in config or str(config[k]) not in filter_data[k]:
                    logging.info(
                        f"    -> Skipped because of filter on {k} ({'missing' if k not in config else str(config[k]) + '!=' + str(filter_data[k])})")
                    return None

        # remove ignore columns
        if ignore_cols is not None:
            for key in ignore_cols:
                if key in config:
                    del config[key]

        # Construct a global configuration including sweep data
        for key in config.keys():
            if key not in global_config:
                global_config[key] = [config[key]]
            else:
                # We already have a config parameter for that key. Is it a sweep?
                if config[key] not in global_config[key]:
                    global_config[key].append(config[key])

        # And return the fresh config
        return config
