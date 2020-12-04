===============================
ireland_smartmeterdata
===============================

.. Comment out Badges until implemented...
    image:: https://img.shields.io/travis/rdmolony/ireland_smartmeterdata.svg
        :target: https://travis-ci.org/rdmolony/ireland_smartmeterdata
.. 
    image:: https://circleci.com/gh/rdmolony/ireland_smartmeterdata.svg?style=svg
        :target: https://circleci.com/gh/rdmolony/ireland_smartmeterdata
.. 
    image:: https://codecov.io/gh/rdmolony/ireland_smartmeterdata/branch/master/graph/badge.svg
        :target: https://codecov.io/gh/rdmolony/ireland_smartmeterdata

This repository contains a collection of

1. `Python` scripts to wrangle datasets into a usable format (with the help of `prefect`, `dask` and `pandas`)
2. `Jupyter Notebooks` to run these scripts and setup a sandbox in which it is straightforward to query the cleaned data for insights

for the following datasets: 

- `CER Electricity Customer Behaviour Trial 2009-2010`__
- `CER Gas Customer Behaviour Trial 2009-2010`__

__ https://www.ucd.ie/issda/data/commissionforenergyregulationcer/ 
__ https://www.ucd.ie/issda/data/commissionforenergyregulationcer/


Installation
------------

To setup the `ireland_smartmeterdata` sandbox for the CER Customer Behaviour Trial datasets:

- Request and download the CER Customer Behaviour trial data.

- Google Collab:
    - Unzip and upload the dataset to `Google Drive`
    - Click the Google Collab badge:
    
        .. image:: https://colab.research.google.com/assets/colab-badge.svg
                :target: https://colab.research.google.com/github/codema-dev/ireland_smartmeterdata
                
    - Mount your Google Drive to your Google Collab instance & refresh your filetree

        .. image:: images/link-google-drive.PNG
    
    - Copy the path to your Google Drive data folder and paste it into the appropriate string

        .. image:: images/copy-path.png

    - For more information see `External data: Local Files, Drive, Sheets, and Cloud Storage`__
    
    __ https://colab.research.google.com/notebooks/io.ipynb

- Local:
    - Unzip the dataset
    - Clone this repository locally via :code:`git clone https://github.com/codema-dev/ireland_smartmeterdata` 
    - Launch `Jupyter Notebook` and open the relevant sandbox file in the `notebooks` folder 
