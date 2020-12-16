#!/usr/bin/env python3
# coding=utf-8

import logging
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(
    # Figure out the path of the evaluation directory
    os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
)

from evals.base_evaluate import experiments_directory, get_evaluation_params, finish_evaluation, EvalParams, \
    foreach_sweep_constellation
from helpers.data_table_mappers import eval_after_24h_mapper, get_eval_cut_off
from helpers.experiment_loader import load_multiple_experiments
from helpers.visualization_helpers import plot_violin, \
    set_paper_aesthetics

logging.basicConfig(level=logging.INFO, format='%(levelname)8s: %(message)s -- in %(filename)s:%(lineno)d')

if __name__ == '__main__':
    # Load the experiment name and output directory from program args
    params: EvalParams = get_evaluation_params(
        tmp_directory_suffix="paper-resource-usage",
        name="paper-resource-usage"
    )

    # We are interested in the cell and scheduler dumps
    types = [
        "cell-inp", "cell"
    ]

    # The used inp properties
    inp_properties = [
        # 0 # not used
        1,  # Sharp
        2,  # IncBricks
        3,  # NetCache
        4,  # DistCache
        5,  # NetChain
        6,  # Harmonia
        7,  # HovercRaft
        8  # R2P2
    ]
    # The number of dimensions for switch resources
    res_dims = 3


    def col_name_util(prop, dim):
        """
        load of this prop on the activated switches, [0-100]
        """
        return f"UtilizationSwitch_Prop{prop}_Dim{dim}"


    def col_name_total(prop, dim):
        """
        how many switches are activated with this property? [0-100]
        """
        return f"TotalSwitch_Prop{prop}_Dim{dim}"


    def col_name_blocked(prop, dim):
        """
        load of other props running on same switches as this prop [0-100]
        """
        return f"BlockedByOthersSwitch_Prop{prop}_Dim{dim}"


    def dyn_col_prop_active(prop):
        """
        how many switches are activated with this property? [0-100]
        """
        return f"ActiveSwitchesWith_Prop{prop}"


    def dyn_col_load_prop_dim(prop, dim):
        """
        load (per dim) of the switches with this property active (load of this prop + load of other props)
        """
        return f"LoadSwitches_Prop{prop}_Dim{dim}"


    def dyn_col_load_prop(prop):
        """
        load of the switches with this property active (load of this prop + load of other props)
        """
        return f"LoadSwitches_Prop{prop}"


    def dyn_col_name_total_dim(dim):
        """
        how many switches are activated for a given dimension? [0-100* max-props-per-switch]
        """
        return f"DynTotalSwitch_Dim{dim}"


    def dyn_col_unused_dim(dim):
        """
        unused resources per dim
        """
        return f"Unused_Switches_Dim{dim}"


    active_switches_field_names = []
    utilization_field_names = []
    load_field_names = []
    load_dim_field_names = []
    unused_res_field_names = []
    other_field_names = []
    for dimension in range(0, res_dims):
        unused_res_field_names.append(dyn_col_unused_dim(dimension))
        other_field_names.append(dyn_col_name_total_dim(dimension))
    for property in inp_properties:
        active_switches_field_names.append(dyn_col_prop_active(property))
        load_field_names.append(dyn_col_load_prop(property))
        for dimension in range(0, res_dims):
            utilization_field_names.append(col_name_util(property, dimension))
            other_field_names.append(col_name_total(property, dimension))
            other_field_names.append(col_name_blocked(property, dimension))

            load_dim_field_names.append(dyn_col_load_prop_dim(property, dimension))

    all_cols = active_switches_field_names + utilization_field_names + load_field_names + other_field_names + \
               load_dim_field_names + unused_res_field_names

    print(f"Fields I will load: {all_cols}")


    def cell_inp_load_mapper(load):
        tmp_col = "tmp_total_used"
        load[tmp_col] = 0

        for dim in range(0, res_dims):
            load[dyn_col_unused_dim(dim)] = 0
            load[dyn_col_name_total_dim(dim)] = 0

        for prop in inp_properties[:]:
            for dim in range(0, res_dims):
                if col_name_total(prop, dim) not in load:
                    logging.warning(f"switch property {prop} with dimension {dim} not found in data! ({load.keys()})")
                    if prop in inp_properties:
                        inp_properties.remove(prop)
                        prop = None
                        break
            if prop is None:
                continue

            # hold active switches for each prop
            load[dyn_col_prop_active(prop)] = load[col_name_total(prop, 0)]
            # prepare aggregated load
            load[dyn_col_load_prop(prop)] = 0
            for dim in range(0, res_dims):
                load[dyn_col_load_prop_dim(prop, dim)] = load[col_name_util(prop, dim)] + load[
                    col_name_blocked(prop, dim)]
                # add load of all dimensions
                load[dyn_col_load_prop(prop)] += load[dyn_col_load_prop_dim(prop, dim)]

                # get unused resources
                load[dyn_col_unused_dim(dim)] += (1 - load[dyn_col_load_prop_dim(prop, dim)]) * load[
                    col_name_total(prop, dim)]
                load[dyn_col_name_total_dim(dim)] += load[col_name_total(prop, dim)]

            # get load average
            load[dyn_col_load_prop(prop)] /= res_dims

        for dim in range(0, res_dims):
            load[dyn_col_unused_dim(dim)] /= load[dyn_col_name_total_dim(dim)] + 0.01

        return load


    mappers = {
        # Apply some default transformations
        "cell-inp": [
            eval_after_24h_mapper,
            get_eval_cut_off(params),
            cell_inp_load_mapper],
        "cell": [eval_after_24h_mapper, get_eval_cut_off(params)]
    }

    columns = {
        "cell-inp": ["Time", "scheduler"] + all_cols,
        "cell": ["UtilizationServersDim0", "UtilizationServersDim1",
                 "UtilizationSwitchesDim0", "UtilizationSwitchesDim1",
                 "UtilizationSwitchesDim2", "ActualUtilizationSwitchesDim2"] + ["Time", "scheduler"]
    }

    if 'mu-inp' not in params.filter:
        logging.warning("strictly enforce filter of mu-inp=1.0")
        params.filter['mu-inp'] = ["1.0"]

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
        data_inc = dataframes["cell-inp"]
        data_server = dataframes["cell"]

        if "experiment" not in data_inc.keys():
            data_inc["experiment"] = 0
            data_server["experiment"] = 0

        name_dim_0 = "Dim-Switch Recirc."
        name_dim_1 = "Dim-Switch Stages"
        name_dim_2 = "Dim-Switch SRAM"

        name_sdim_0 = "Dim-Server CPU"
        name_sdim_1 = "Dim-Server Mem"

        data_inc[name_dim_0] = (1 - data_inc[dyn_col_unused_dim(0)]) * 100
        data_inc[name_dim_1] = (1 - data_inc[dyn_col_unused_dim(1)]) * 100
        data_inc[name_dim_2] = (1 - data_inc[dyn_col_unused_dim(2)]) * 100

        data_server[name_sdim_0] = data_server["UtilizationServersDim0"] * 100
        data_server[name_sdim_1] = data_server["UtilizationServersDim1"] * 100

        name_dim_bottleneck = "Bottleneck Dimension"
        data_inc[name_dim_bottleneck] = np.amax(data_inc[[name_dim_0, name_dim_1, name_dim_2]].to_numpy(),
                                                axis=1)

        data_inc = data_inc[config['sweeps'] + ['Time',
                                                'experiment',
                                                'scheduler',
                                                'run',
                                                'scheduler-strategy'] + [name_dim_0,
                                                                         name_dim_1,
                                                                         name_dim_2,
                                                                         name_dim_bottleneck]]
        data_all = pd.merge(data_inc, data_server, on=config['sweeps'] + ['Time',
                                                                          'experiment',
                                                                          'scheduler',
                                                                          'run',
                                                                          'scheduler-strategy'])

        for tmp in [name_dim_0, name_dim_1, name_dim_2, name_sdim_0, name_sdim_1]:
            data_all[tmp[len("Dim-"):]] = data_all[tmp]

        plot_violin(
            columns=[name_dim_0[len("Dim-"):],
                     name_dim_1[len("Dim-"):],
                     name_dim_2[len("Dim-"):]],
            hue="scheduler",
            data=data_all,
            y_label="Load [%]",
            name=f"plot-resource-usage-switch-dims-{i}",
            params=params
        )

        sw_stages = "Switch StagesNN@ µ=100%"
        data_all[sw_stages] = data_all[name_dim_1[len("Dim-"):]]

        plot_violin(
            columns=[sw_stages],
            hue="scheduler",
            data=data_all,
            y_label="Load [%]",
            name=f"plot-resource-usage-stages-dims-{i}",
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
