import logging
import math
import re
import sys

from evals.base_evaluate import EvalParams

assert sys.version_info >= (3, 6)
import os
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker

# sns.set(style="darkgrid")
sns.set_style("whitegrid")

sns.set_context("paper",
                font_scale=1.3,
                rc={"lines.linewidth": 1.7})
# Keep all plots in mem. If this does not help, we might need to use cla()
plt.rcParams.update({'figure.max_open_warning': 0,
                     # 'figure.autolayout': False
                     })

col_pal = sns.color_palette("tab20", n_colors=20)


class COLS:
    @classmethod
    def c(cls, id: str):
        if re.match(r'[ABCDEFGHIJ]{1}[0-9]{1}', id) is None:
            raise NameError(f"invalid color requested: {id}")
        cat = ord(id[0])  # A has 65
        n = int(id[1])

        def col_tint(col, factor):
            return [x + (1 - x) * factor for x in col]

        def col_shade(col, factor):
            return [x * (1 - factor) for x in col]

        base_col = (cat - 65) * 2
        if n > 3:
            base_col += 1

        col = col_pal[base_col]
        n_effective = n % 4
        if n_effective > 0:
            col = col_tint(col, 0.15 * n_effective)

        return col


marker_default = '.'
marker_yarn = 'v'  # v
marker_k8 = 'D'  # |
marker_sparrow = 'o'  # +
marker_hire = 'P'  #
marker_coco = '>'  #

dash_default = [2, 0]
dash_greedy = [2, 1]
dash_resubmit = [7, 1, 1, 1, 1, 1, 1, 1, 1, 1]
dash_hire_alternative = [1, 1]

scheds = [
    ['hire,parallel-0-relax-cost-ssp', "HIRE", COLS.c("D0"), marker_hire, dash_default],
    ['hire,parallel-0-relax-cost-ssp--simple-flavor', "HIRE (simple flavor)", COLS.c("D2"), marker_hire,
     dash_hire_alternative],
    ['coco,parallel-0-relax-cost-ssp,resubmit,-10', 'CoCo++ Timeout', COLS.c("H0"), marker_coco, dash_resubmit],
    ['kubernetes#,greedy', "K8++ Concurrent", COLS.c("A0"), marker_k8, dash_greedy],
    ['kubernetes,resubmit,-10', "K8++ Timeout", COLS.c("A2"), marker_k8, dash_resubmit],
    ['sparrow,2,200,0.5,true,greedy', "Sparrow++ Concurrent", COLS.c("C0"), marker_sparrow, dash_greedy],
    ['sparrow,2,200,0.5,false,resubmit,-10', "Sparrow++ Timeout", COLS.c("C2"), marker_sparrow, dash_resubmit],
    ['yarn#,greedy', "Yarn++ Concurrent", COLS.c("E0"), marker_yarn, dash_greedy],
    ['yarn,resubmit,-10', "Yarn++ Timeout", COLS.c("E2"), marker_yarn, dash_resubmit]
]

sched_to_pretty_names = {e[0]: e[1] for e in scheds}

sorted_scheds = [e[0] for e in scheds]
sorted_scheduler_strategies = ["hire", "resubmit", "greedy", "unknown"]
sched_to_dashes_dict = {e[0]: e[4] for e in scheds}
sched_to_markers_dict = {e[0]: e[3] for e in scheds}

for sched in sorted_scheds:
    if sched not in sched_to_pretty_names:
        raise NameError(f"scheduler '{sched}' is missing in sched_to_pretty_names")
print(f"Sorted schedulers: {' '.join(sorted_scheds)}")

sched_pallete = {e[0]: e[2] for e in scheds}

print(f"Color pallet: {sched_pallete.items()}")

sched_pallete_pretty_names = {sched_to_pretty_names[k]: sched_pallete[k] for k in sched_pallete}


def set_paper_aesthetics(font_scale=1.6, line_width=1.55):
    sns.set_style("whitegrid")
    sns.set_style("ticks")

    dark_gray = ".15"
    light_gray = ".8"

    sns.set_context(
        "paper",
        font_scale=font_scale,
        rc={"lines.linewidth": line_width,
            "figure.facecolor": "white",
            "axes.labelcolor": dark_gray,
            # "text.usetex": True,

            "xtick.direction": "out",
            "ytick.direction": "out",
            "xtick.color": dark_gray,
            "ytick.color": dark_gray,

            # 'figure.figsize': (4.3, 3.15),

            "axes.axisbelow": True,
            "grid.linestyle": "-",

            "text.color": dark_gray,
            "font.family": ["sans-serif"],
            "font.sans-serif": ["Arial", "DejaVu Sans", "Liberation Sans",
                                "Bitstream Vera Sans", "sans-serif"],

            "lines.solid_capstyle": "round",
            "patch.edgecolor": "w",
            "patch.force_edgecolor": True,
            'figure.autolayout': False,
            "image.cmap": "rocket",
            'axes.grid': True,
            # 'grid.linestyle': '-',
            "axes.edgecolor": dark_gray,
            "lines.markersize": 10.0,
            "lines.markeredgewidth": 1.0,
            # "image.aspect": 10.0,
            "xtick.bottom": True,
            "ytick.left": True,
            "axes.facecolor": "white",
            "grid.color": light_gray,

            "axes.spines.left": True,
            "axes.spines.bottom": True,
            "axes.spines.right": True,
            "axes.spines.top": True,
            # 'xtick.major.size': 8,
            # 'ytick.major.size': 8
            })

    plt.rcParams.update({'figure.max_open_warning': 0,
                         # 'figure.autolayout': False
                         })


def plot_text(params: EvalParams, text: str):
    # figure out how many lines we have to write
    lines = text.split("\n")
    # per 1 y unit, we write 6 lines
    y_size = len(lines) // 6 + 1
    if y_size > 8:
        print("WARNING... not yet tested with many lines")
    fig_width = 8
    fig = plt.figure(figsize=(fig_width, y_size))
    r = fig.canvas.get_renderer()
    plt.setp(plt.gca(), frame_on=False, xticks=(), yticks=())
    y_offset = 1 - .16 / y_size

    for l in lines:
        t = plt.text(0.02, y_offset, l, transform=fig.transFigure, size=10)
        bb = t.get_window_extent(renderer=r)
        fb = fig.get_window_extent(renderer=r)

        if bb.width > fb.width:
            fig_width *= round(bb.width / fb.width, 1) + 0.1
            fig.set_size_inches(fig_width, y_size)

        # update y offset to write next line.. based on total size of page
        y_offset -= .16 / y_size
    # save current page
    params.pdf.savefig()
    plt.close()


def plot_rel_multiple(x, y, x_label, y_label, hue, data, name, params: EvalParams, log_x=True, log_y=True,
                      reverse_hue_order=False,
                      custom_size_height=3.15,
                      custom_size_aspect=4.3 / 3.15):
    prep_fig()

    hue_uniques = data[hue].unique()

    if hue == "scheduler":
        this_hue_order = [tmp for tmp in sorted_scheds if tmp in hue_uniques]
        this_pallete = sched_pallete
    else:
        this_hue_order = sorted(hue_uniques)
        this_pallete = {tmp: "black" for tmp in this_hue_order}

    if reverse_hue_order:
        this_hue_order.reverse()
    print(f"hue order: {this_hue_order}")

    g = sns.relplot(
        x=x,
        y=y,
        hue=hue,
        palette=this_pallete,
        kind="line",
        data=data,
        hue_order=this_hue_order,
        style=hue,
        dashes=sched_to_dashes_dict if hue == "scheduler" else True,
        markers=sched_to_markers_dict if hue == "scheduler" else True,
        markevery=3,
        legend="brief",
        height=custom_size_height,
        aspect=custom_size_aspect
    )

    for ax in g.axes[-1, :]:
        ax.grid(which='major')

    if log_y:
        g.set(yscale='log')
        for ax in g.axes[:, 0]:
            loc_maj = matplotlib.ticker.LogLocator(base=10, numticks=10)
            # ax.grid(which='major')
            ax.yaxis.set_major_locator(loc_maj)
            loc_min = matplotlib.ticker.LogLocator(base=10, subs=(.1, .2, .3, .4, .5, .6, .7, .8, .9), numticks=10)
            ax.yaxis.set_minor_locator(loc_min)
            ax.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())

    if log_x:
        g.set(xscale='log')
        for ax in g.axes[-1, :]:
            loc_maj = matplotlib.ticker.LogLocator(base=10, numticks=10)
            # ax.grid(which='major')
            ax.xaxis.set_major_locator(loc_maj)
            loc_min = matplotlib.ticker.LogLocator(base=10, subs=(.1, .2, .3, .4, .5, .6, .7, .8, .9), numticks=10)
            ax.xaxis.set_minor_locator(loc_min)
            ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())

    g.set(xlabel=x_label, ylabel=y_label)

    plt.tight_layout()

    plt.savefig(os.path.join(params.plots_directory, name + ".pdf"))
    params.pdf.savefig()


def prep_fig():
    size = (8.5, 7)
    plt.gca().set_axis_off()
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0,
                        hspace=0, wspace=0)
    plt.margins(0, 0)
    plt.figure(figsize=size)
    return size


def plot_line_multiple(styles, x, hue, data, name, params: EvalParams, ylog=False, x_label=None,
                       y_label=None, ci=95, plot_type="line",
                       reverse_hue_order=False, custom_x_limit=None, custom_y_limit=None, this_hue_order=None):
    logging.info(f"Plotting {y_label} by {x_label} as lineplot")

    prep_fig()

    # Melt the columns into their own column
    # id_vars = [hue, x, "scheduler-strategy"]
    id_vars = [hue, x]

    # Only keep those columns that we are interested in
    data = data[id_vars + styles]

    data = data.melt(
        id_vars=id_vars,
        var_name='origin_column',
        value_name='y'
    )

    # print(data["scheduler-strategy"].unique())

    hue_uniques = data[hue].unique()
    hue_unique_cnt = len(hue_uniques)

    if hue == "scheduler":
        for z in hue_uniques:
            if z not in sorted_scheds:
                logging.error(f"Unknown scheduler: {z}")

    if hue == "scheduler":
        this_hue_order = [tmp for tmp in sorted_scheds if tmp in hue_uniques]
        this_pallete = sched_pallete
    else:
        this_hue_order = this_hue_order if this_hue_order is not None else sorted(hue_uniques)
        this_pallete = {tmp: "black" for tmp in this_hue_order}

    print(f"hue order: {this_hue_order}")

    style_col = hue
    style_order = this_hue_order

    print(f"use style col: {style_col}")

    if x == "mu-inp":
        data[x] = data[x] * 100.0

    if plot_type == "line":
        g = sns.lineplot(
            x=x,
            y='y',
            hue=hue,
            hue_order=this_hue_order,
            palette=this_pallete,
            legend="brief",
            style=style_col,
            dashes=sched_to_dashes_dict if hue == "scheduler" else True,
            markers=sched_to_markers_dict if hue == "scheduler" else True,
            markevery=4,
            err_style="band",
            ci=ci,
            style_order=style_order,
            data=data
        )
    else:
        raise NameError(f"invalid option: {plot_type}")

    g.grid(which='major')

    if custom_x_limit is not None:
        g.set_xlim(custom_x_limit[0], custom_x_limit[1])
    if custom_y_limit is not None:
        g.set_ylim(custom_y_limit[0], custom_y_limit[1])

    # Set log scala if desired
    if ylog:
        g.set(yscale='log')

        # do we need to set limits manually?
        limits = g.get_ylim()
        lower = math.log10(limits[0])
        upper = math.log10(limits[1])
        print(f"lower: {lower} upper: {upper}")
        if abs(lower - upper) < 1.0:
            # increase upper limit?
            if int(lower) == int(upper):
                upper = 1 + int(upper)
            lower = int(lower)
            print(f"new: {math.pow(10, lower), math.pow(10, upper)}")
            g.set(ylim=(math.pow(10, lower), math.pow(10, upper)))

        loc_maj = matplotlib.ticker.LogLocator(base=10, numticks=10)
        g.yaxis.set_major_locator(loc_maj)

        loc_min = matplotlib.ticker.LogLocator(base=10, subs=(.1, .2, .3, .4, .5, .6, .7, .8, .9), numticks=10)
        g.yaxis.set_minor_locator(loc_min)
        g.yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())

    if hue == "scheduler":
        handles, labels = g.get_legend_handles_labels()
        g.legend(
            handles=handles[1:hue_unique_cnt + 1],
            labels=[sched_to_pretty_names[x] for x in labels[1:hue_unique_cnt + 1]])

    # Provide names for the axis
    g.set(xlabel=x_label, ylabel=y_label)

    plt.tight_layout()

    plt.savefig(os.path.join(params.plots_directory, name + ".pdf"), bbox_inches='tight',
                pad_inches=0)
    params.pdf.savefig()

    if params.show_plots:
        plt.show()
    else:
        plt.close()


def plot_violin(columns, hue, data, name,
                params: EvalParams,
                rotate_x=45,
                y_label=None):
    prep_fig()

    # Melt the columns into their own column
    id_vars = [hue]

    hue_uniques = data[hue].unique()

    this_hue_order = None
    if hue == "scheduler":
        for z in hue_uniques:
            if z not in sorted_scheds:
                logging.error(f"Unknown scheduler: {z}")

        this_hue_order = [tmp for tmp in sorted_scheds if tmp in hue_uniques]
        print(f"hue order: {this_hue_order}")

    # Only keep those columns that we are interested in
    data = data[id_vars + columns]

    data = data.melt(
        id_vars=id_vars,
        var_name='origin_column',
        value_name='y'
    )

    g = sns.violinplot(x="origin_column",
                       y="y",
                       hue=hue,
                       hue_order=this_hue_order,
                       palette=sched_pallete if hue == "scheduler" else None,
                       data=data,
                       dodge=True,
                       cut=0,
                       split=False,
                       scale="area",
                       inner="quartile")

    if rotate_x is not None:
        plt.xticks(rotation=rotate_x, horizontalalignment='center')

    handles, labels = g.get_legend_handles_labels()
    g.legend(
        handles=handles,
        labels=[sched_to_pretty_names[x] for x in labels])

    # Provide names for the axis
    g.set(xlabel=None, ylabel=y_label)

    plt.tight_layout()

    plt.savefig(os.path.join(params.plots_directory, name + ".pdf"))
    params.pdf.savefig()

    if params.show_plots:
        plt.show()
    else:
        plt.close()
