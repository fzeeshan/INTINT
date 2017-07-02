try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'INTINT',
    'author': 'Vladimir Ivanov',
    'url': '',
    'download_url': 'Where to download it.',
    'author_email': 'chtcvl@gmail.com',
    'version': '0.1',
    'install_requires': ['nose'],
    'packages': ['NAME'],
    'scripts': [],
    'name': 'INTINT'
}

setup(**config)
