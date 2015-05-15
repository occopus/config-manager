#!/usr/bin/env -e python

import setuptools
from pip.req import parse_requirements

setuptools.setup(
    name='OCCO-ServiceComposer',
    version='0.1.0',
    author='Adam Visegradi',
    author_email='adam.visegradi@sztaki.mta.hu',
    namespace_packages=['occo',
                        'occo.servicecomposer',
                        'occo.servicecomposer.backends'],
    py_modules=['occo.servicecomposer.servicecomposer',
                'occo.servicecomposer.backends.chef'],
    scripts=[],
    url='http://www.lpds.sztaki.hu/',
    license='LICENSE.txt',
    description='OCCO Service Composer',
    long_description=open('README.txt').read(),
    install_requires=[
        'pychef',
        'OCCO-Util',
        'OCCO-InfoBroker',
    ]
)
