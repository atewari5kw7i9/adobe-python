from setuptools import setup, find_packages

__version__ = '1.0.0'

setup(
    name='adobe-pyspark',
    version=__version__,
    packages=find_packages(),
    author="Aditya S Tewari",
    author_email="aditya.tewari@yahoo.com",
    install_requires=['pyspark', 'boto3']
)

