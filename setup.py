import subprocess
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

def get_version():
    try:
        return subprocess.check_output(['git', 'describe', '--tags', '--always']).strip().decode("utf-8")
    except:
        return "0.0.dev0"

setup(
    name='crawlstreams',
    version=get_version(),
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    license='Apache 2.0',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            'submit=crawlstreams.submit:main',
            'crawlstreams=crawlstreams.report:main',
            'analyse=crawlstreams.analysis:main',
            'launcher=crawlstreams.launcher:main',
            'streamer=crawlstreams.streamer:main',
        ]
    }
)
