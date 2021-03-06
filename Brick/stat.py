import matplotlib as mpl
# mpl.use('Agg')
import matplotlib.pyplot as plt
import json


def setup_mpl():
    fig_width_pt = 800  # Get this from LaTeX using \showthe\columnwidth
    inches_per_pt = 1.0 / 72.27  # Convert pt to inch
    fig_width = fig_width_pt * inches_per_pt  # width in inches
    fig_height = fig_width_pt * inches_per_pt * 0.618  # height in inches
    fig_size = [fig_width, fig_height]
    params = {'figure.figsize': fig_size,
              'savefig.dpi': 600}
    mpl.rcParams.update(params)


def state_finish_time(path):
    with open(path) as f:
        data = json.load(f)

    start_time = data["start_time"]
    data = data["task_time"]
    l = []
    ts = set()

    for k, v in data.iteritems():
        t, i = k.split("-")
        i = int(i.strip("[]"))
        l.append((i, t, v[0] - start_time, v[1] - start_time))
        if t not in ts:
            ts.add(t)

    l.sort(key=lambda x: x[0])
    # plot_finish_time(l, ts)
    plot_num_task(l, ts)


def plot_finish_time(l, ts):
    setup_mpl()
    fig = plt.figure()
    fig.canvas.set_window_title("ST/FT/RT of tasks (tasks sorted by ST)")
    ax1 = fig.add_subplot(211)
    ax1.set_ylabel('Time (s)')

    ps = sorted(l, key=lambda x: x[2])
    # ps = l
    start_ps = [x[2] for x in ps]
    finish_ps = [x[3] for x in ps]
    ax1.plot(range(0, len(ps)), start_ps)
    ax1.plot(range(0, len(ps)), finish_ps)

    names = []
    lines = []
    for t in ts:
        tps = [x for x in ps if x[1] == t]
        xs = [ps.index(x) for x in tps]
        ys = [y[3] for y in tps]
        lines.append(ax1.plot(xs, ys, ".")[0])
        names.append(t)

    ax1.legend(lines, names, loc='lower right', ncol=2, numpoints=1)

    ax2 = fig.add_subplot(212, sharex=ax1)
    rts = [y[3] - y[2] for y in ps]
    ax2.plot(range(0, len(ps)), rts)
    ax2.set_ylabel('Time(s)')

    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.tight_layout()
    plt.show()


def plot_num_task(l, ts):
    setup_mpl()
    fig = plt.figure()
    fig.canvas.set_window_title("Tasks completed by time")
    plt.ylabel('Tasks completed')
    plt.xlabel("Time (s)")

    ps = sorted(l, key=lambda x: x[3])
    xs = [x[3] for x in ps]
    ys = [ps.index(y) + 1 for y in ps]

    plt.plot(xs, ys)

    plt.tight_layout()
    plt.show()


def state_each_time(files):
    points = {}

    for path in files:
        with open(path) as f:
            data = json.load(f)

        for k, v in data.items():
            task_type = k.split("-")[0]
            for service_type, rt in v.iteritems():
                t = "%s@%s" % (task_type, service_type)
                if t not in points:
                    points[t] = [rt]
                else:
                    points[t].append(rt)

    plot_stat(points)


def plot_stat(points):
    setup_mpl()
    fig, ax = plt.subplots()
    fig.canvas.set_window_title("RT of tasks on services.")
    plt.xlabel('Time(s)')
    ax.set_xscale('log')

    count = 1
    ks = sorted(points.keys())
    for k in ks:
        v = points[k]
        ax.plot(v, [count] * len(v), "o")
        count += 1

    ax.yaxis.set_ticks(range(1, count))
    ax.yaxis.set_ticklabels(ks)
    ax.set_ylim(0, count)
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    # state_finish_time("test/MoWSC/mowsc_exp.run")
    # state_finish_time("test/MoWSC/mowsc_exp.run.local")
    state_each_time(["test/MoWSC/mowsc_exp.time", "test/MoWSC/mowsc_exp.time.local"])
