#!/usr/bin/env python
# Compile and upload on pip:
#    python setup.py bdist_wheel
#    python -m twine upload dist/*

from setuptools import find_namespace_packages
from distutils.core import setup

setup(
    name='cima.goes',
    version='1.1.b80',
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
    install_requires=[
        'psutil==5.7.0',
        'netCDF4==1.5.3',
        'google-cloud-storage==1.28.1',
        'aioftp==0.16.0',
        'aiofiles==0.5.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)