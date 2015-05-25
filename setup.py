import re
import ast
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('snakemq_pubsub/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name='snakeMQ-PubSub',
    version=version,
    url='https://github.com/repole/snakemq-pubsub',
    download_url="https://github.com/repole/snakemq-pubsub/tarball/" + version,
    license='MIT',
    author='Nicholas Repole',
    author_email='n.repole@gmail.com',
    description='An implementation of the publish-subscribe pattern for snakeMQ.',
    packages=['snakemq_pubsub'],
    platforms='any',
    test_suite='snakemq_pubsub.tests',
    tests_require=[
        'snakeMQ>=1.2'
    ],
    install_requires=[
        'snakeMQ>=1.2'
    ],
    keywords=['snakeMQ', 'pubsub'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Operating System :: OS Independent"
    ]
)