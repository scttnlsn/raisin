from setuptools import setup, find_packages

version = '0.1'

setup(
    name = 'raisin',
    version = version,
    description = 'Library for storing, retrieving and indexing JSON objects in MySQL',
    author = 'Scott Nelson',
    author_email = 'scottbnel@gmail.com',
    url = 'http://www.github.com/scottbnel/raisin/',
    packages = find_packages(),
    zip_safe = True,
)
