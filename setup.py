from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
    "dask[dataframe]",
    "prefect",
    "pyarrow",
]

setup(
    name='ireland_smartmeterdata',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="A collection of scripts to wrangle Ireland Smart Meter Data into any desired format",
    license="MIT",
    author="Rowan Molony",
    author_email='rowan.molony@codema.ie',
    url='https://github.com/rdmolony/ireland_smartmeterdata',
    packages=['ireland_smartmeterdata'],
    entry_points={
        'console_scripts': [
            'ireland_smartmeterdata=ireland_smartmeterdata.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='ireland_smartmeterdata',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
