from setuptools import setup, find_packages
import os
import re
import codecs

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

with open('test_requirements.txt', 'r') as f:
    test_required = f.read().splitlines()


dependency_links = []
del_ls = []
for i_l in range(len(required)):
    l = required[i_l]
    if l.startswith("-e"):
        dependency_links.append(l.split("-e ")[-1])
        del_ls.append(i_l)

        required.append(l.split("=")[-1])

for i_l in del_ls[::-1]:
    del required[i_l]

setup(
    version='1.0.1',
    name='materializationengine',
    description="Combines DynamicAnnotationDB and PyChunkedGraph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Forrest Collman',
    author_email='forrestc@alleninstitute.org',
    url='https://github.com/seung-lab/MaterializationEngine',
    packages=find_packages(),
    include_package_data=True,
    install_requires=required,
    setup_requires=['pytest-runner'],
    tests_require=test_required,
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
    ),
    dependency_links=dependency_links)
