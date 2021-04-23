from setuptools import setup, find_packages
from ddbmodel import VERSION

REQUIREMENTS = [
    'boto3',
    'botocore',
    'dynamodb_json'
]
TEST_REQUIREMENTS = [
    'pep8>=1.7.0'
]
EXCLUDE_ITEMS = [
    '*.pyc', '__pycache__', '*.tests', '*.tests.*', 'tests.*', 'tests'
]

setup(
    name='ddbmodel',
    version=VERSION,
    description='AWS DynamoDB Model',
    url='',
    author='Shiv Pratap Singh',
    packages=find_packages(exclude=EXCLUDE_ITEMS),
    install_requires=REQUIREMENTS,
    tests_require=TEST_REQUIREMENTS,
    setup_requires=['pytest-runner'],
    include_package_data=True,
    zip_safe=False
)
