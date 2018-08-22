import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

version = '0.3.1'

requires = [
    'pyyaml',
    'couchdb',
    'requests',
    'openprocurement_client',
    'dpath',
    'pytz',
    'isodate',
    'redis',
    'statsdhandler'
]

test_require = {
    'test': [
        'pytest',
        'pytest-mock',
        'pytest-cov',
        'lazydb'
    ]
}

entry_points = {
    'console_scripts': [
        'concierge_worker = openregistry.concierge.worker:main'
    ],
    'openregistry.pytests': [
        'concierge = openregistry.concierge.tests.main:suite'
    ]
}

setup(
    name='openregistry.concierge',
    version=version,
    description="openregistry.concierge",
    long_description=README,
    classifiers=[
      "Framework :: Pylons",
      "License :: OSI Approved :: Apache Software License",
      "Programming Language :: Python",
      "Topic :: Internet :: WWW/HTTP",
      "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
    ],
    keywords="web services",
    author='Quintagroup, Ltd.',
    author_email='info@quintagroup.com',
    license='Apache License 2.0',
    packages=find_packages(exclude=['ez_setup']),
    namespace_packages=['openregistry'],
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    extras_require=test_require,
    entry_points=entry_points
)
