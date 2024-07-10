from setuptools import setup

setup(
    name='kubequery',
    version='0.1',
    description='a rest api to query the graph built by kubegrapher',
    author='Tarek Zaarour',
    author_email='tarek.zaarour@dell.com',
    packages=['kubequery'],
    zip_safe=False
)