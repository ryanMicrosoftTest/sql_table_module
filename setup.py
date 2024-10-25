from setuptools import setup, find_packages

setup(
    name='sql_table_module',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'pyodbc',
        'azure-keyvault-secrets',
        'azure-identity'
    ],
)