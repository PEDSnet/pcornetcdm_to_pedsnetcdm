from setuptools import setup

with open('requirements.txt') as f:
    install_requires = f.readline()

setup(
    name='PCORnet to PEDSnet Transform',
    version='1.0',
    py_modules=['loading_pedsnet'],
    summary='This package can be used for PCORnet to PEDSnet ETL',
    install_requires=[
        install_requires
    ],
    entry_points='''
	    [console_scripts]
	    loading_pedsnet=loading_pedsnet.main:cli
	'''
)
