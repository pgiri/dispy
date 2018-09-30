import sys
import os
import re
from setuptools import setup

if sys.version_info.major == 3:
    base_dir = 'py3'
else:
    assert sys.version_info.major == 2
    assert sys.version_info.minor >= 7
    base_dir = 'py2'

with open(os.path.join(base_dir, 'dispy', '__init__.py')) as fd:
    for line in fd:
        if line.startswith('__version__ = '):
            module_version = re.match(r'[^\d]+([\d\.]+)', line).group(1)

setup(
    name='dispy',
    version=module_version,
    description='Distributed and Parallel Computing with/for Python.',
    keywords='distributed computing, parallel processing, mapreduce, hadoop, job scheduler',
    url='http://dispy.sourceforge.net',
    author='Giridhar Pemmasani',
    author_email='pgiri@yahoo.com',
    package_dir={'':base_dir},
    packages=['dispy'],
    package_data = {
        'dispy' : ['data/*', 'examples/*', 'doc/*'],
    },
    install_requires=['pycos >= 4.8.3'],
    scripts=[os.path.join(base_dir, 'dispy', script) for script in \
             ['dispynode.py', 'dispynetrelay.py', 'dispyscheduler.py']] + \
            [os.path.join(base_dir, script) for script in ['dispy.py']],
    license='Apache 2.0',
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.1',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development',
        ]
    )
