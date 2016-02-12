import json

import sh

from Brick.engine import LimitEngine
from Brick.provider.local import ProcessProvider
from Brick.provider.qing import QingProvider
from Brick.workflow import Workflow
from plot import mpl_plot_front, mpl_plot_track, mpl_plot_hvs
from util import *

base_path = "./"

dag_path = base_path + "dag/"
output_path = base_path + "output/"
plot_path = base_path + "plot/"
support_path = base_path + "support/"
info_path = support_path + "ec2.json"
hv_module_path = support_path + "hv.py"

workflows = {"CYBERSHAKE": [30, 50, 100]}
# "MONTAGE": [25, 50, 100, 1000]}
algorithms = ["spea2_star", "esc_p"]  # , "esc_f", "esc_nh", "moabc"]
hv_reference_point = [1.1, 1.1]

w = Workflow(disabled=False)


@w.create_task()
def generate_xml(app, size):
    xml_path = dag_path + "%s_%d.xml" % (app, size)
    wf_generator = sh.Command(support_path + "WorkflowGenerator/bin/AppGenerator")
    wf_generator("-a%s" % app, "--", n=size, _out=xml_path)
    return xml_path


@w.create_task()
def convert_dag(xml_path):
    json_path = xml_path[:-4] + ".json"
    convert_program = sh.Command(support_path + "convert")
    convert_program(xml_path, _out=json_path)
    return json_path


@w.create_task()
def experiment(alg, dag_name, dag_file, n):
    output_file = output_path + "%s_%s.%d.json" % (dag_name, alg, n)
    exp_program = sh.Command(support_path + "exp")
    exp_program(alg, dag_file, info_path, p=50, s=10, _out=output_file)
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

    hv = get_hv_func(hv_module_path, hv_reference_point)
    for result in results:
        res = norm(result["results"], bounds)
        hv_value = hv.compute(res)
        if hv_value > best_hv:
            best_hv = hv_value
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
    return get_hv_func(hv_module_path, hv_reference_point).compute(res)


@w.create_task()
def compute_track(res_file, bounds):
    track = []
    with open(res_file) as f:
        res = json.load(f)
    hv = get_hv_func(hv_module_path, hv_reference_point)
    for mid_res in res["extra"]["trace"]:
        mid_res = norm(mid_res, bounds)
        track.append(hv.compute(mid_res))
    return track


@w.create_task()
def plot_fronts(dag_name, res_files):
    results = {}
    for k, v in res_files.iteritems():
        with open(v) as f:
            res = json.load(f)
        results[k] = pareto_filter(res["results"])
    plot_file = plot_path + dag_name + ".fronts.png"
    mpl_plot_front(results, plot_file)
    return plot_file


@w.create_task()
def plot_track(dag_name, history_hvs):
    plot_file = plot_path + dag_name + ".track.png"
    mpl_plot_track(history_hvs, plot_file)
    return plot_file


@w.create_task()
def plot_hv(app, hvs):
    plot_file = plot_path + app + ".hvs.png"
    mpl_plot_hvs(hvs, plot_file)
    return plot_file


p = QingProvider(api_keypath="../access_key.csv",
                 zone="pek2",
                 image="img-7yorhmuq",
                 keypair="kp-p2h7c1sp",
                 vxnets="vxnet-0domhwj")


@LimitEngine(ProcessProvider(), 4, workflow=w)
# @FullEngine(ProcessProvider(), workflow=w)
def mowsc_exp(wf_args, repeat=2):
    for app, numbers in wf_args.iteritems():
        hvs = {}
        for size in numbers:
            dag_name = "%s_%d" % (app, size)
            xml_path = generate_xml(app, size)
            dag_file = convert_dag(xml_path)

            results = {}
            for alg in algorithms:
                multi_results = [experiment(alg, dag_name, dag_file, i)
                                 for i in range(repeat)]
                results[alg] = best_run(multi_results)

            bounds = find_bounds(results)
            history_hvs = {alg: compute_track(results[alg], bounds)
                           for alg in algorithms}

            plot_fronts(dag_name, results)
            plot_track(dag_name, history_hvs)

            hvs[size] = {alg: compute_hv(results[alg], bounds)
                         for alg in algorithms}

        plot_hv(app, hvs)


if __name__ == '__main__':
    import time

    start_time = time.time()
    mowsc_exp(workflows, 4)
    print "Used %fs" % (time.time() - start_time)
