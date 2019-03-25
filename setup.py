import sys
import os
import re
import shutil
from setuptools import setup

if sys.version_info.major == 2:
    assert sys.version_info.minor >= 7
    base_dir = 'py2'
else:
    assert sys.version_info.major >= 3
    if sys.version_info.major == 3 and sys.version_info.minor < 7:
        base_dir = 'py3'
    else:
        base_dir = 'py3.7'

if len(sys.argv) > 1 and sys.argv[1] == 'sdist':
    assert os.path.isdir('py3')
    shutil.rmtree('py3.7', ignore_errors=True)
    shutil.copytree('py3', 'py3.7')
    for path in ['py3.7', os.path.join('py3.7', 'dispy')]:
        for filename in os.listdir(path):
            if filename.endswith('.py'):
                filename = os.path.join(path, filename)
                sbuf = os.stat(filename)
                with open(filename, 'r') as fd:
                    lines = [line.replace('sys.version_info.minor < 7',
                                          'sys.version_info.minor >= 7')
                             if line.endswith('sys.version_info.minor < 7, \\\n')
                             else line.replace('raise StopIteration', 'return')
                             for line in fd]
                with open(filename, 'w') as fd:
                    fd.write(''.join(lines))
                os.chmod(filename, sbuf.st_mode)

with open(os.path.join(base_dir, 'dispy', '__init__.py')) as fd:
    regex = re.compile(r'^__version__ = "([\d\.]+)"$')
    for line in fd:
        match = regex.match(line)
        if match:
            module_version = match.group(1)
            break

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
    install_requires=['pycos >= 4.8.11'],
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
