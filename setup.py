from setuptools import setup, find_packages
import os
import re
import codecs

here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

with open("README.md", "r") as fh:
    long_description = fh.read()

with open('requirements.txt', 'r') as f:
    required = f.read().splitlines()

with open('test_requirements.txt', 'r') as f:
    test_required = f.read().splitlines()


setup(
    version=find_version("materializationengine", "__init__.py"),
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
    ),)

