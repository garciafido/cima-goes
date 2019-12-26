#!/usr/bin/env python
# Compile and upload on pip:
#    python setup.py bdist_wheel
#    python -m twine upload dist/*

from setuptools import find_namespace_packages
from distutils.core import setup

setup(
    name='cima.goes',
    version='1.1.b71',
    description='GOES-16 File Processing',
    author='Fido Garcia',
    author_email='garciafido@gmail.com',
    package_dir={'': 'src'},
    url='https://github.com/garciafido/cima-goes',
    packages=find_namespace_packages(where='src'),
    include_package_data=True,
    license='MIT',
    package_data={'': ['*.json', '*.cpt']},
    data_files = [("", ["LICENSE"])],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)