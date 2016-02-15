from Brick import Workflow
from Brick.engine import SingleEngine
from Brick.provider.local import ProcessProvider
import sh, os

dirs = filter(os.path.exists, os.environ["PATH"].split(":"))

@SingleEngine(ProcessProvider())
def merge(dirs):
    w = Workflow()

    @w.create_task()
    def my_ls(i, d):
        file_name = "bin_%d.txt" % i
        sh.ls("-l", d, _out=file_name)
        return file_name

    @w.create_task()
    def my_cat(files):
        return str(sh.cat(*files))

    return my_cat([my_ls(i, d) for i, d in enumerate(dirs)])

if __name__ == '__main__':
    for line in merge(dirs).splitlines():
        if "brick" in line:
            print line