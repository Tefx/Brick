from setuptools import setup, find_packages

requires = ["gevent", "sh", "python-snappy", "Husky", "tabulate", "gipc", "networkx", "decorator"]
console_scripts = ['brick-worker=Brick.worker:run_worker',
                   'brick-test-worker=Brick.worker:test_worker',
                   'brick-ls=Brick.tools:list_status']

setup(
        name='Brick',
        version='0.1',
        packages=find_packages(),
        url='https://github.com/Tefx/Brick',
        license='GPL v3',
        author='Zhaomeng Zhu',
        author_email='zhaomeng.zhu@gmail.com',
        description='scripting and running scientific workflows in Python',
        install_requires=["gevent", "sh", "python-snappy", "Husky", "tabulate", "gipc", "networkx"],
        entry_points=dict(console_scripts=console_scripts)
)
