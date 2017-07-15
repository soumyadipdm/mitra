
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'Mitra - a friendly tool to discover your infrastructure via Elasticsearch',
    'author': 'Soumyadip Das Mahapatra',
    'author_email': 'soumyadip.bt@gmail.com',
    'version': '0.1',
    'install_requires': [
        'elasticsearch',
        'elasticsearch-dsl',
        'python-daemon',
        'PyYAML'],
    'packages': ['mitra', 'mitra.indexer', 'mitra.searcher'],
    'scripts': [],
    'name': 'mitra'}

setup(**config)
