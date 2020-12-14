import logging
from collections import namedtuple
from typing import List

import numpy as np
import pandas as pd

from evals.base_evaluate import EvalParams
from helpers.statistics_globals import config_columns_except_seed


def scheduling_success_mapper(data):
    if "CountJobsWithInp" not in data:
        logging.warning("'CountJobsWithInp' not in dataset, fake this metrics "
                        "- WARNING - this tend to show higher INC success ratios")
        data["CountJobsWithInp"] = data["TimesServerFlavorTaken"] + data["TimesSwitchFlavorTaken"] + data[
            "TimesServerFallbackResubmit"] + (data["CountJobs"] - data["TotalJobsFullyScheduled"] - data[
            "PendingJobsFullyScheduled"])

    data["EffectiveCountJobs"] = data["CountJobs"] - data["TimesServerFallbackResubmit"]

    if "PastSystemMs" in data:
        data["SimSpeed"] = 0
        data.loc[data['PastSystemMs'] > 0, "SimSpeed"] = (data["Time"] / 1000) / data["PastSystemMs"] * 100

        data["SimSpeedCapped"] = data["SimSpeed"]
        data.loc[data['SimSpeedCapped'] > 100, "SimSpeedCapped"] = 100

        data["SimSpeedRolling"] = data["PastSystemMs"].diff().fillna(0)
        data.loc[data['SimSpeedRolling'] > 0, "SimSpeedRolling"] = (data["Time"].diff() / 1000) / data[
            "SimSpeedRolling"] * 100
        data.loc[data['SimSpeedRolling'] > 100, "SimSpeedRolling"] = 100

    data["AllocationsRolling"] = data["Allocations"].diff().fillna(0)
    data["AllocationsRolling"] = 100.0 * data["AllocationsRolling"] / max(data["AllocationsRolling"])
    data["CountPreemptionsRolling"] = data["CountPreemptions"].diff().fillna(0)
    data["CountPreemptionsRolling"] = 100.0 * data["CountPreemptionsRolling"] / max(data["CountPreemptionsRolling"])

    # INC allocated over total allocated
    data["AllocatedIncSuccess"] = data["TotalJobsFullyScheduledINP"] / (1 + data["CountJobsWithInp"]) * 100.0
    # INC taken over possible choices
    data["RollingIncTaken"] = data["TimesSwitchFlavorTaken"] / (1 + data["CountJobsWithInp"]) * 100.0

    data["WastedAllocations"] = 100.0 * data["CountPreemptions"] / (
            data["ScheduledTasks"] + data["CountPreemptions"])

    data["SchedulingSuccess"] = 100.0 * (data["TotalJobsFullyScheduled"] + data["PendingJobsFullyScheduled"]) / (
        data["EffectiveCountJobs"])

    return data


def remove_withdrawn_jobs_from_tenant_statistics(data):
    # W withdrawn
    # D done
    # A fully allocated
    # O ongoing, not fully allocated
    return data[data["JobStatus"] != "W"]


# Calculates some basic metrics from the given columns
def solver_time_mapper(solvers):
    # How long did the run last?
    solvers["ElapsedTimeNanos"] = solvers["EndTimeNanos"] - solvers["StartTimeNanos"]
    # Convert Nanos to Ms for better readability
    solvers["ElapsedTimeMs"] = solvers["ElapsedTimeNanos"] / 1000000
    # Calculate the total node count
    solvers["NodeCnt"] = solvers["ProducerCnt"] + solvers["NonProducerCnt"]

    return solvers


SummarizeColSpecs = namedtuple('SummarizeColSpecs', ['column', 'agg_columns', 'filt'])


def summarize_cdf(params: EvalParams,
                  col: str):
    # we need to share the bins across the seeds...
    shared_bins_store = {}

    def x(data):
        # create config hash for shared bins
        config_hash = "".join([f"{tmp}:{data[tmp].iloc[0]}" for tmp in
                               sorted([str(tmp) for tmp in data.keys() if tmp in config_columns_except_seed])])

        # check if data is corrupted
        assert (col in data.keys())
        assert (len(data["run"].unique()) == 1)

        col_cdf = f"{col}_cdf_value"
        col_ccdf = f"{col}_cdf_cvalue"
        col_bins = f"{col}_cdf_bucket"

        df = data.sort_values(by=col)
        d = df[col]

        # use log bins
        subdivisions = 100

        if config_hash in shared_bins_store:
            logging.info(f"used stored logbins for doing cdf for config hash: {config_hash}")
            logbins = shared_bins_store[config_hash]
            real_min = d.min()
            real_max = d.max()
            if real_min < logbins[0]:
                logging.info(f" update lower bin for config ({logbins[0]} -> {real_min}): {config_hash}")
                logbins = np.insert(logbins, 0, float(real_min))
                shared_bins_store[config_hash] = logbins
            if real_max > logbins[-1]:
                logging.info(f" update upper bin for config ({logbins[-1]} -> {real_max}): {config_hash}")
                logbins = np.insert(logbins, len(logbins), float(real_max))
                shared_bins_store[config_hash] = logbins

        else:
            logging.info(f"create logbin cache for: {config_hash}")
            logbins = np.logspace(
                np.log10(d.min()),
                np.log10(d.max()),
                num=subdivisions)
            shared_bins_store[config_hash] = logbins

        hist, bins = np.histogram(d, bins=logbins)
        histCdf = hist.cumsum() / hist.sum()

        percentiles = np.percentile(d, [75, 90, 95, 99])

        logging.info(f" describe -> {d.describe()}")
        logging.info(f" percentiles -> {percentiles}")
        if "SubmissionTime" in df.keys():
            logging.info(f"   99tg -> {df[df[col] > percentiles[-1]]['SubmissionTime'].describe()}")

        out = pd.DataFrame()
        out[col_cdf] = histCdf
        out[col_ccdf] = 1 - histCdf
        out[col_bins] = bins[:-1]

        # print(f"min: {histCdf.min()} max:{histCdf.max()}")

        for tmp in data.keys():
            if tmp != col:
                out[tmp] = data[tmp].iloc[0]

        return out

    return x


def summarize_5tuple_mapper(params: EvalParams,
                            summarize_columns: List[SummarizeColSpecs]):
    def percentil25(x):
        return np.percentile(x, q=25)

    def percentil95(x):
        return np.percentile(x, q=95)

    def percentil99(x):
        return np.percentile(x, q=99)

    def x(data):
        out = None

        for col_spec in summarize_columns:

            # simply migrate old style calls
            if not isinstance(col_spec, SummarizeColSpecs):
                col_spec = SummarizeColSpecs(column=col_spec, agg_columns=[], filt=None)

            this_data = data
            if col_spec.filt is not None:
                this_data = col_spec.filt(this_data)

            df = this_data.groupby(["run"] + col_spec.agg_columns)
            df_agg = df.agg({col_spec.column:
                                 [np.mean, np.median, percentil25, percentil95, percentil99]}).reset_index(drop=False)

            df_agg.columns = ["_".join(z) if len(z[1]) > 0 else z[0] for z in df_agg.columns.ravel()]

            if out is None:
                out = df_agg
            else:
                out = out.merge(df_agg, how="outer")

        # add all the missing values
        for key in data.keys():
            if key not in out.keys():
                out[key] = data[key].iloc[0]

        # out = out.drop(["run"], axis=1)
        return out

    return x


def eval_remove_unfinished_taskgroups(params: EvalParams):
    def x(data):
        return data[data["TotalTasks"] == data["TasksStarted"]]

    return x


def task_group_inc_success_mapper(data):
    if data["scheduler-strategy"].unique()[0] != "hire":
        return data[data["scheduler-strategy"] == "hire"]

    if "FlavorInp" not in data.keys():
        logging.warning("new 'FlavorInp' key not present in statistics, mimic data")
        # return data[data["JobStatus"] == "NOTHING"]
        data["FlavorInp"] = "Y"
        data.loc[(data["TaskGroupType"] == "S"), "FlavorInp"] = "N"

    if "ValidForJob" not in data.keys():
        logging.warning("new 'ValidForJob' key not present in statistics, mimic data")
        data["ValidForJob"] = "Y"
        data.loc[(data["JobStatus"] == "W"), "ValidForJob"] = "N"

    # count tgs with flavor
    # data[data["ValidForJob"] == "Y"]]
    flavorTgs = len(data[(data["FlavorInp"] == "Y") | (data["FlavorInp"] == "N")])
    incTgs = len(data[(data["FlavorInp"] == "Y") &
                      (data["ValidForJob"] == "Y") &
                      (data["JobStatus"] != "W")])

    df_data = {'TaskGroupsWithFlavor': [flavorTgs],
               'TaskGroupsIncAllocated': [incTgs],
               'AllocatedIncSuccess': [100.0 * incTgs / flavorTgs]}

    out = pd.DataFrame(df_data)

    newCols = out.keys()
    # add all config keys
    for tmp in data.keys():
        if tmp not in newCols:
            out[tmp] = data[tmp].iloc[0]

    assert (len(out) == 1)

    return out


def eval_tasks_not_started_mapper(params: EvalParams):
    # NOT_STARTED_TIMEOUT = 20 * 60 * 1000  # 10 minutes

    # ignore last 1 second
    NOT_STARTED_TIMEOUT_FIX = 1000
    NOT_STARTED_THRESHOLD = params.cut_time - NOT_STARTED_TIMEOUT_FIX

    def x(data):
        # remove switch task groups from statistics, if they belong to a job which finally ended up as a server job
        len_before = len(data)
        if "ValidForJob" in data.keys():
            data = data.loc[data["ValidForJob"] == "Y"]

        else:
            logging.warning("new 'ValidForJob' key not present in statistics, mimic same behavior")
            data = data.loc[(data["JobStatus"] != "N") | (data["TaskGroupType"] != "I")]

        len_after = len(data)
        logging.info(f"removed invalid entries (preempted allocations that are not part of the job anymore)"
                     f" {len_before} -> {len_after}")

        data["TasksNotStarted"] = data["TotalTasks"] - data["TasksStarted"]
        data.loc[data['TasksNotStarted'] == 0, "PlacementLatency"] = data["PlacementLatency"] - data["SubmissionTime"]
        data.loc[(data['TasksNotStarted'] > 0) & (
                data["SubmissionTime"] < NOT_STARTED_THRESHOLD), "PlacementLatency"] = params.cut_time - data[
            "SubmissionTime"]
        data.loc[(data['TasksNotStarted'] > 0) & (
                data["SubmissionTime"] >= NOT_STARTED_THRESHOLD), "PlacementLatency"] = NOT_STARTED_TIMEOUT_FIX
        data["CompletionTime"] = data["PlacementLatency"] + data["Duration"]

        data["CompletionTimeS"] = data["CompletionTime"] / 1000
        data["CompletionTimeM"] = data["CompletionTimeS"] / 60

        return data

    return x


def cut_all_but_last_measurements(data):
    if "Time" in data.columns:
        latest = data["Time"].max()
        return data[data["Time"] >= latest]
    return data


def eval_after_24h_mapper(data):
    timestamp_24h = 86400000

    # If this belongs to scheduler/cell stats
    for time_key in ["Time", "SubmissionTime", "SimulationTime"]:
        if time_key in data.columns:
            return data.loc[data[time_key] >= timestamp_24h]

    return data


def get_eval_cut_off(params: EvalParams):
    timestamp_cut_off = int(params.cut_time)

    def eval_cut_off(data):
        # If this belongs to scheduler/cell stats
        for time_key in ["Time", "SubmissionTime", "SimulationTime"]:
            if time_key in data.columns:
                return data.loc[data[time_key] < timestamp_cut_off]

        return data

    return eval_cut_off
