# Nielsen Retail Reader


| | |
| --- | --- |
| License | [![License](https://img.shields.io/badge/LICENSE-blue)](https://github.com/pratikrelekar/NielsenDSRS/blob/main/LICENSE) |
| Dependencies | [![Pandas](https://img.shields.io/badge/Pandas-navy)](https://pandas.pydata.org) \| [![Dask Latest Release](https://img.shields.io/badge/Dask-orange)](https://www.dask.org) \| [![Distributed](https://img.shields.io/badge/Distributed-yellow)](https://distributed.dask.org) \| [![Numpy](https://img.shields.io/badge/Numpy-green)](https://numpy.org) \| [![Toolz](https://img.shields.io/badge/Toolz-red)](https://github.com/pytoolz/toolz)\| [![Msgpack](https://img.shields.io/badge/Msgpack-brown)](https://msgpack.org)
| Meta | [![PyPI](https://img.shields.io/badge/PyPI-turquoise)](https://pypi.org)




## Overview:

**Nielsen Retail Reader’s** is a special-purpose library and it's main purpose is to facilitate ease of processing of Nielsen Retail Scanner data of Kilt’s Center’s Nielsen IQ data used for Academic research only. The striking feature of this library is Dask which acts as an underlying framework that uniquely empowers the user to read Nielsen data with limited on device resources (by processing larger-than-memory data in chunks and distributed fashion). It understands the Kilts/Nielsen directory structure.

## Data:
Information about the Retail Scanner data can be found here: [**Kilts Center for Marketing**](https://www.chicagobooth.edu/research/kilts/research-data/nielseniq)

## IMPORTANT:

### Access to Nielsen Retail Data:

Please note that Nielsen retail data is proprietary and access is restricted to individuals whose institutions have an existing subscription or agreement with Nielsen. If you intend to use this library for accessing and analyzing Nielsen data, you must first ensure that you are authorized to do so by your institution. Unauthorized access or use of this data may violate terms of use and could have legal implications. Nielsen dataset should strictly follow standard naming convention as per laid out by Nielsen and Kilts Center of Marketing and under no circumstances the naming convention should be changed.

**NielsenRetail** processes Retail Scanner Data.

## Table of Contents

- [Main Features](#main-features)
- [Where to get it](#where-to-get-it)
- [Dependencies](#dependencies)
- [License](#license)
- [Background](#background)
- [Getting Help](#getting-help)
- [Discussion and Development](#discussion-and-development)

## Main Features
Here are just a few of the things that NielsenRetail does well:
  - Efficiently manages Nielsen directory and hierarchy, simplifying the process for researchers and significantly reducing the time needed to navigate through Nielsen documentation.
  - Size mutability: Processes dataframes [**larger-than-memory**](https://examples.dask.org/dataframe.html) on a single machine through batch processing.
  - Distributed computing for terabyte sized datasets enhancing the overall data reading speed by utlising [**low-latency**](https://distributed.dask.org/en/stable/efficiency.html) feature of Dask.
  - Provides simple yet distinct commands for separating sales, stores, and products data for analysis purposes.
  - This package has excellent compatibility with [**Numpy**](https://numpy.org), [**Pandas**](https://pandas.pydata.org), [**Scikit-learn**](https://scikit-learn.org/stable/), [**SQL databases** like Postgres](https://www.postgresql.org) etc.


## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/pratikrelekar/NielsenDSRS

Binary installers will be available at [Python Package Index (PyPI)](https://pypi.org/)

For Github pip install:
```sh
pip install git+https://github.com/pratikrelekar/NielsenDSRS
```

For pip install requirements:
```sh
python -m pip install -r requirements.txt
```


## Dependencies

Before using NielsenRetail, ensure that all dependencies are correctly installed. Additionally, verify that the Client hosting the Python environment, the Scheduler, and the Worker nodes all have the same version installed.

- [NumPy - Adds support for large, multi-dimensional arrays, matrices and high-level mathematical functions to operate on these arrays](https://pypi.org/project/numpy/1.26.3/)
- [Pandas - Provides high-performance, easy-to-use data structures, and data analysis tools.](https://pypi.org/project/pandas/2.2.0/)
- [Dask - Flexible parallel computing library for analytic computing, enabling performance at scale for the tools of your choice](https://pypi.org/project/dask/2024.1.1/)
- [Dask Distributed - Enables parallel computing and scaling to clusters for large computations, enhancing Dask’s capabilities to work across multiple machines by distributing tasks and managing workloads efficiently](https://pypi.org/project/distributed/2024.1.1/)
- [Toolz - Provides functional utilities for working with iterable data, enabling more efficient and readable data processing by offering a set of pure functions inspired by constructs from functional programming](https://pypi.org/project/toolz/0.12.0/)
- [Msgpack - Binary serialization format that allows for efficient, compact storage and is used for exchanging data between multiple languages, similar to JSON but faster and smaller](https://pypi.org/project/msgpack/1.0.7/)


## License

[MIT License](https://github.com/pratikrelekar/NielsenDSRS/blob/main/LICENSE)


## Background

This library was developed at [**Data Science Research Services(University of Illinois at Urbana-Champaign)**](https://dsrs.illinois.edu) in 2024 and has been under active development since then. Currently supports Nielsen Retail Scanner data from 2006 to 2020.




