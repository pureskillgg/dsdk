from sys import argv

from setuptools import find_packages, setup

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(argv)
pytest_runner = ['pytest-runner>=4.0.0,<5.0.0'] if needs_pytest else []

with open('README.rst', 'r') as f:
    long_description = f.read()

setup(
    name='makenew-pypackage',
    version='1.0.0',
    author='Evan Sosenko',
    author_email='razorx@evansosenko.com',
    packages=find_packages(exclude=['docs']),
    url='https://github.com/makenew/pypackage',
    license='MIT',
    description='Package skeleton for a python module.',
    long_description=long_description,
    tests_require=[
        'pytest>=4.0.0,<5.0.0',
    ],
    install_requires=[
    ] + pytest_runner
)