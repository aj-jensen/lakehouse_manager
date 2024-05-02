from setuptools import setup, find_packages

setup(
    name='dremio_actions',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'certifi>=2023.7.22',
        # 'commoncode>=1.0',
        'idna>=3.4',
        'python-dateutil>=2.8.2',
        'pytz>=2023.3.post1',
        'setuptools>=68.2.0',
        'six>=1.16.0',
        'requests>=2.31.0',
        'urllib3<2.0.0',
        'wheel>=0.38.4'
    ]
)
