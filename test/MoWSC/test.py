import imp
import json

import sh

from Brick.engine import LimitEngine
from Brick.provider.local import ProcessProvider as LocalProcessProvider
from Brick.workflow import Workflow

base_path = "./data/"
wf_generator = sh.Command(base_path + "WorkflowGenerator/bin/AppGenerator")
convert_program = sh.Command(base_path + "MoWSC/convert")
exp_program = sh.Command(base_path + "MoWSC/exp")
info_path = base_path + "MoWSC/info.json"
hv_module_path = base_path + "hv.py"

workflows = {"CYBERSHAKE": [100]}  # , "MONTAGE": [100, 1000]}
algorithms = ["spea2_star", "esc_p"]  # , "esc_f", "esc_nh", "moabc"]
hv_reference_point = [1.1, 1.1]

w = Workflow(disabled=False)


def norm(l, bounds):
    x_max, x_min, y_max, y_min = bounds
    x_delta = x_max - x_min
    y_delta = y_max - y_min
    l2 = []
    for x, y in l:
        x = (x - x_min) / x_delta
        y = (y - y_min) / y_delta
        l2.append((x, y))
    return l2


def update_bounds(result, bounds):
    if not bounds:
        x_max = float("-inf")
        x_min = float("inf")
        y_max = float("-inf")
        y_min = float("inf")
    else:
        x_max, x_min, y_max, y_min = bounds
    for x, y in result:
        x_min = x if x_min > x else x_min
        x_max = x if x_max < x else x_max
        y_min = y if y_min > y else y_min
        y_max = y if y_max < y else y_max
    return x_max, x_min, y_max, y_min


def get_hver():
    module = imp.load_source("hv", hv_module_path)
    return module.HyperVolume(hv_reference_point)


@w.create_task()
def generate_xml(app, size):
    xml_path = base_path + "dag/%s_%d.xml" % (app, size)
    wf_generator("-a%s" % app, "--", n=size, _out=xml_path)
    return xml_path


@w.create_task()
def convert_dag(xml_path):
    json_path = xml_path[:-4] + ".json"
    convert_program(xml_path, _out=json_path)
    return json_path


@w.create_task()
def experiment(alg, dag_file, n):
    output_file = "%s_%s.%d.json" % (dag_file[:-5], alg, n)
    exp_program(alg, dag_file, info_path, p=20, s=10, _out=output_file)
    return output_file


@w.create_task()
def best_run(output_files):
    results = []
    bounds = None

    for filename in output_files:
        with open(filename) as f:
            result = json.load(f)
        bounds = update_bounds(result["results"], bounds)
        results.append(result)

    best_res = None
    best_hv = 0

    for result in results:
        res = norm(result["results"], bounds)
        hv = get_hver().compute(res)
        if hv > best_hv:
            best_hv = hv
            best_res = result

    best_file = output_files[0][:-7] + ".json"
    with open(best_file, "w") as f:
        json.dump(best_res, f)

    return best_file


@w.create_task()
def find_bounds(results):
    bounds = None
    for filename in results.itervalues():
        with open(filename) as f:
            result = json.load(f)
        bounds = update_bounds(result["results"], bounds)
    return bounds


@w.create_task()
def compute_hv(res_file, bounds):
    with open(res_file) as f:
        res = json.load(f)
    res = norm(res["results"], bounds)
    return get_hver().compute(res)


@w.create_task()
def compute_track(res_file, bounds):
    track = []
    with open(res_file) as f:
        res = json.load(f)
    hver = get_hver()
    for mid_res in res["extra"]["trace"]:
        mid_res = norm(mid_res, bounds)
        track.append(hver.compute(mid_res))
    return track


@w.create_task()
def plot_hv(app, hvs):
    return NotImplemented


@w.create_task()
def plot_track(dag_name, history_hvs):
    return NotImplemented


@w.create_task()
def plot_fronts(dag_name, results):
    return NotImplemented


@LimitEngine(LocalProcessProvider(), 4, workflow=w)
def mowsc_exp(wf_args, repeat=2):
    for app, numbers in wf_args.iteritems():
        hvs = {}
        for size in numbers:
            print app, size
            dag_name = "%s_%d" % (app, size)
            xml_path = generate_xml(app, size)
            dag_file = convert_dag(xml_path)

            results = {}
            for alg in algorithms:
                multi_results = [experiment(alg, dag_file, i)
                                 for i in range(repeat)]
                results[alg] = best_run(multi_results)

            bounds = find_bounds(results)
            history_hvs = {alg: compute_track(results[alg], bounds)
                           for alg in algorithms}

            # plot_fronts(dag_name, results)
            # plot_track(dag_name, history_hvs)

            hvs[size] = {alg: compute_hv(results[alg], bounds)
                         for alg in algorithms}
            # plot_hv(app, hvs)


if __name__ == '__main__':
    mowsc_exp(workflows, 4)
