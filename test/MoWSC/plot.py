def setup_mpl():
    import matplotlib as mpl
    mpl.use('Agg')
    fig_width_pt = 600  # Get this from LaTeX using \showthe\columnwidth
    inches_per_pt = 1.0 / 72.27  # Convert pt to inch
    fig_width = fig_width_pt * inches_per_pt  # width in inches
    fig_height = fig_width_pt * inches_per_pt * 0.618  # height in inches
    fig_size = [fig_width, fig_height]
    params = {'figure.figsize': fig_size,
              'savefig.dpi': 1200}
    mpl.rcParams.update(params)
    import matplotlib.pyplot as plt
    return plt


def mpl_plot_front(results, output):
    plt = setup_mpl()
    fig, ax = plt.subplots()
    plt.xlabel('Time(s)')
    plt.ylabel('Cost(\$)')
    ax.set_xscale('log')

    lines = []
    names = []
    for k, v in results.iteritems():
        x, y = zip(*sorted(v))
        lines.append(ax.plot(x, y, "*-")[0])
        names.append(k)

    plt.legend(lines, names, numpoints=1)
    plt.tight_layout()
    plt.savefig(output, format="png")


def mpl_plot_track(results, output):
    plt = setup_mpl()
    fig, ax = plt.subplots()
    plt.xlabel('Generation')
    plt.ylabel('Hypervolume')

    lines = []
    names = []
    for k, v in results.iteritems():
        lines.append(ax.plot(v)[0])
        names.append(k)

    ax.set_xlim(0, len(results.values()[0]))
    plt.legend(lines, names, loc="lower right")
    plt.tight_layout()
    plt.savefig(output, format="png")


def mpl_plot_hvs(hvs, output):
    plt = setup_mpl()
    fig, ax = plt.subplots()
    plt.xlabel('Number of tasks')
    plt.ylabel('Hypervolume')

    lines = []

    ns = sorted(hvs.keys())
    algs = hvs[ns[0]].keys()

    for alg in algs:
        y = [hvs[x][alg] for x in ns]
        lines.append(plt.plot(ns, y, ".-")[0])

    ax.set_xlim(ns[0] - 10, ns[-1] + 10)
    ax.set_ylim(0, 1.2)

    plt.legend(lines, algs, loc="lower right")
    plt.tight_layout()
    plt.savefig(output, format="png")
