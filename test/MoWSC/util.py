import imp


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


def dominate(a, b):
    if a[0] < b[0]:
        return a[1] <= b[1]
    elif a[0] == b[0]:
        return a[1] == b[1]
    else:
        return False


def pareto_filter(res):
    p_res = []
    for p in res:
        if not any(dominate(x, p) for x in p_res):
            p_res.append(p)
    return p_res


def get_hv_func(hv_module_path, hv_reference_point):
    return imp.load_source("hv", hv_module_path).HyperVolume(hv_reference_point)
