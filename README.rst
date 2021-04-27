================================
CER Smart Meter Trials 2009-2011
================================

This repository simplifies working with:

- `CER Electricity Customer Behaviour Trial 2009-2010`__
- `CER Gas Customer Behaviour Trial 2010-2011`__

__ https://www.ucd.ie/issda/data/commissionforenergyregulationcer/ 
__ https://www.ucd.ie/issda/data/commissionforenergyregulationcer/

... with the help of the open source Python software `dask`

... via:

- A `Google Colaboratory` Jupyter Notebook sandbox environment
- A `cer` Python library


Installation
------------

To setup the sandbox:

- `Request and download the CER Customer Behaviour trial data`__

__ https://www.ucd.ie/issda/data/commissionforenergyregulationcer/ 

- Click the Google Collab badge below & open `sandbox.ipynb`:

    .. image:: https://colab.research.google.com/assets/colab-badge.svg
            :target: https://colab.research.google.com/github/codema-dev/cer-smart-meter-trials-2009-2011
            
Note
====

`cer-smart-meter-trials-2009-2011` assumes the following folder structure for the data files::

    ├── CER Electricity Revised March 2012
    │   ├── CER_Electricity_Data
    │   │   ├── Survey data - CSV format
    │   │   │   ├── Smart meters Residential post-trial survey data.csv
    │   │   │   ├── Smart meters Residential pre-trial survey data.csv
    │   │   │   ├── Smart meters SME post-trial survey data.csv
    │   │   │   └── Smart meters SME pre-trial survey data.csv
    │   │   └── Survey data - Excel format
    │   │       ├── Smart meters Residential post-trial survey data.xlsx
    │   │       ├── Smart meters Residential pre-trial survey data.xlsx
    │   │       ├── Smart meters SME post-trial survey data.xlsx
    │   │       └── Smart meters SME pre-trial survey data.xlsx
    │   ├── CER_Electricity_Documentation
    │   │   ├── Electricity Smart Metering Technology Trials Findings Report.pdf
    │   │   ├── Manifest - Smart Meter Electricity Trial Data v1.0.1.docx
    │   │   ├── RESIDENTIAL POST TRIAL SURVEY.doc
    │   │   ├── RESIDENTIAL PRE TRIAL SURVEY.doc
    │   │   ├── SME POST TRIAL SURVEY.doc
    │   │   ├── SME PRE TRIAL SURVEY.doc
    │   │   ├── SME and Residential allocations.xlsx
    │   │   └── ~$SME and Residential allocations.xlsx
    │   ├── File1.txt.zip
    │   ├── File2.txt.zip
    │   ├── File3.txt.zip
    │   ├── File4.txt.zip
    │   ├── File5.txt.zip
    │   └── File6.txt.zip
    └── CER Gas Revised October 2012
        ├── CER_Gas_Data
        │   ├── GasDataWeek 0
        │   ├── GasDataWeek 1
        │   ├── GasDataWeek 10
        │   ├── GasDataWeek 11
        │   ├── GasDataWeek 12
        │   ├── GasDataWeek 13
        │   ├── GasDataWeek 14
        │   ├── GasDataWeek 15
        │   ├── GasDataWeek 16
        │   ├── GasDataWeek 17
        │   ├── GasDataWeek 18
        │   ├── GasDataWeek 19
        │   ├── GasDataWeek 2
        │   ├── GasDataWeek 20
        │   ├── GasDataWeek 21
        │   ├── GasDataWeek 22
        │   ├── GasDataWeek 23
        │   ├── GasDataWeek 24
        │   ├── GasDataWeek 25
        │   ├── GasDataWeek 26
        │   ├── GasDataWeek 27
        │   ├── GasDataWeek 28
        │   ├── GasDataWeek 29
        │   ├── GasDataWeek 3
        │   ├── GasDataWeek 30
        │   ├── GasDataWeek 31
        │   ├── GasDataWeek 32
        │   ├── GasDataWeek 33
        │   ├── GasDataWeek 34
        │   ├── GasDataWeek 35
        │   ├── GasDataWeek 36
        │   ├── GasDataWeek 37
        │   ├── GasDataWeek 38
        │   ├── GasDataWeek 39
        │   ├── GasDataWeek 4
        │   ├── GasDataWeek 40
        │   ├── GasDataWeek 41
        │   ├── GasDataWeek 42
        │   ├── GasDataWeek 43
        │   ├── GasDataWeek 44
        │   ├── GasDataWeek 45
        │   ├── GasDataWeek 46
        │   ├── GasDataWeek 47
        │   ├── GasDataWeek 48
        │   ├── GasDataWeek 49
        │   ├── GasDataWeek 5
        │   ├── GasDataWeek 50
        │   ├── GasDataWeek 51
        │   ├── GasDataWeek 52
        │   ├── GasDataWeek 53
        │   ├── GasDataWeek 54
        │   ├── GasDataWeek 55
        │   ├── GasDataWeek 56
        │   ├── GasDataWeek 57
        │   ├── GasDataWeek 58
        │   ├── GasDataWeek 59
        │   ├── GasDataWeek 6
        │   ├── GasDataWeek 60
        │   ├── GasDataWeek 61
        │   ├── GasDataWeek 62
        │   ├── GasDataWeek 63
        │   ├── GasDataWeek 64
        │   ├── GasDataWeek 65
        │   ├── GasDataWeek 66
        │   ├── GasDataWeek 67
        │   ├── GasDataWeek 68
        │   ├── GasDataWeek 69
        │   ├── GasDataWeek 7
        │   ├── GasDataWeek 70
        │   ├── GasDataWeek 71
        │   ├── GasDataWeek 72
        │   ├── GasDataWeek 73
        │   ├── GasDataWeek 74
        │   ├── GasDataWeek 75
        │   ├── GasDataWeek 76
        │   ├── GasDataWeek 77
        │   ├── GasDataWeek 8
        │   ├── GasDataWeek 9
        │   ├── Smart meters Residential post-trial survey data - Gas.xls
        │   └── Smart meters Residential pre-trial survey data - Gas.csv
        └── CER_Gas_Documentation
            ├── Gas Customer Behaviour Trial Findings Report.pdf
            ├── Manifest - Smart Meter Gas Trial Data.docx
            ├── RESIDENTIAL POST TRIAL SURVEY - GAS.doc
            ├── RESIDENTIAL PRE TRIAL SURVEY - GAS.doc
            └── Residential allocations.xls
