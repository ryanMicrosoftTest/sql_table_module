from setuptools import setup, find_packages

setup(
    name='table_writer',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'pyodbc',
        'azure-keyvault-secrets',
        'azure-identity'
    ],
)