"""
Should be deployed under the composer's dags/cloudsql_to_bigquery directory
e.g.
gs://europe-west1-production-com-369e4b80-bucket/dags/cloudsql_to_bigquery
"""

import setuptools


REQUIRED_PACKAGES = [
    'psycopg2-binary'
]

setuptools.setup(
    name='main-etl',
    version='0.0.1',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages()
)

