# NielsenIQ Retail Reader


| | |
| --- | --- |
| License | [![License](https://img.shields.io/badge/LICENSE-blue)](https://github.com/pratikrelekar/NielsenDSRS/blob/main/LICENSE) |
| Dependencies | [![PyPI - Version](https://img.shields.io/pypi/v/pandas?logo=pandas&logoColor=blue&label=Pandas&color=blue)](https://pandas.pydata.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/dask?logo=Dask&label=Dask&color=orange)](https://www.dask.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/distributed?logo=dask&logoColor=yellow&label=Distributed&color=yellow)](https://distributed.dask.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/numpy?logo=numpy&logoColor=green&label=Numpy&color=green)](https://numpy.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/toolz?logo=toolz&logoColor=red&label=Toolz&color=red)](https://pypi.org/project/toolz/) \| [![PyPI - Version](https://img.shields.io/pypi/v/pyyaml?label=PyYAML&color=lightgrey)](https://pyyaml.org) |
| Meta | [![PyPI - Version](https://img.shields.io/pypi/v/NielsenIQRetail)](https://pypi.org/project/NielsenIQRetail/) |


## Overview

**NielsenIQ Retail Reader** is a special-purpose library whose main purpose is to facilitate ease of processing of the Kilts Center’s NielsenIQ Retail Scanner data, for academic research only. Its striking feature is **Dask**, the underlying framework that lets the user read NielsenIQ data with limited on-device resources (processing larger-than-memory data in chunks and leveraging distributed computing).

As of **v1.0.0**, a single unified interface reads **both** eras of the Retail Scanner data — the 2006–2020 format **and** the 2021-onward format — harmonizing the product, store, and movement files across the 2020/2021 change in a single `df_sales` with a `batch` column.

> **No data structure ships with this package.** The NielsenIQ file organization and variable names are proprietary and restricted to licensed researchers, so they are **not** distributed here. You supply them once, from your own licensed documentation, via a small schema file (see [Schema configuration](#schema-configuration)). The repository contains processing logic only.

## Data
Information about the Retail Scanner data can be found here: [**Kilts Center for Marketing**](https://www.chicagobooth.edu/research/kilts/research-data/nielseniq)

## IMPORTANT

### Access to NielsenIQ Retail Data

NielsenIQ Retail data is proprietary and access is restricted to individuals whose institutions have an existing subscription or agreement with NielsenIQ. If you intend to use this library for accessing and analyzing NielsenIQ data, you must first ensure that you are authorized to do so by your institution. Unauthorized access or use of this data may violate terms of use and could have legal implications. The NielsenIQ dataset should strictly follow the standard naming convention laid out by NielsenIQ and the Kilts Center for Marketing, and under no circumstances should the naming convention be changed.

**NielsenIQRetail** processes Retail Scanner data.

## Table of Contents

- [Main Features](#main-features)
- [Where to get it](#where-to-get-it)
- [Dependencies](#dependencies)
- [Schema configuration](#schema-configuration)
- [Quick start](#quick-start)
- [Scaling with Dask](#scaling-with-dask)
- [Debug](#debug)
- [License](#license)
- [Background](#background)
- [Getting help](#getting-help)

## Main Features
  - **Reads both data eras** (2006–2020 and 2021-onward) through one interface, with a `batch` column marking the source.
  - Efficiently manages the NielsenIQ directory and hierarchy, reducing the time needed to navigate NielsenIQ documentation.
  - Size mutability: processes [**larger-than-memory**](https://examples.dask.org/dataframe.html) dataframes on a single machine through batch processing.
  - Distributed computing for terabyte-sized datasets, using the [**low-latency**](https://distributed.dask.org/en/stable/) feature of Dask.
  - Simple, distinct commands for sales, stores, products, the 2021-onward product **hierarchy**, and product **attributes**.
  - Excellent compatibility with [**NumPy**](https://numpy.org), [**Pandas**](https://pandas.pydata.org), etc.

## Where to get it
The source code is hosted on GitHub at:
https://github.com/pratikrelekar/NielsenDSRS

For PyPI install:
```sh
pip install NielsenIQRetail
```

For the latest development release:
```sh
pip install git+https://github.com/pratikrelekar/NielsenDSRS
```

To install the requirements:
```sh
python -m pip install -r requirements.txt
```

## Dependencies

Before using NielsenIQRetail, ensure all dependencies are installed. When using a Dask cluster, verify that the Client, Scheduler, and Worker nodes all have the same versions.

- [NumPy](https://pypi.org/project/numpy/1.26.3/) — large, multi-dimensional arrays and high-level math.
- [Pandas](https://pypi.org/project/pandas/2.2.0/) — high-performance data structures and analysis tools.
- [Dask](https://pypi.org/project/dask/2024.1.1/) — flexible parallel computing for analytics at scale.
- [Dask Distributed](https://pypi.org/project/distributed/2024.1.1/) — parallel computing and scaling to clusters.
- [Toolz](https://pypi.org/project/toolz/0.12.0/) — functional utilities for iterable data.
- [PyYAML](https://pypi.org/project/PyYAML/) — reads your schema configuration file.

## Schema configuration

Because the NielsenIQ file layout and variable names are proprietary, they are not shipped with this package — **you provide them once** from your own licensed documentation.

1. Write out the blank template:
   ```python
   import NielsenDSRS
   NielsenDSRS.write_schema_template()      # creates schema.yaml in the current folder
   ```
2. Open `schema.yaml` and fill in the blanks — the column names, data types, attribute variable names, and file/folder names — using the data dictionary and file layout from **your** NielsenIQ/Kilts subscription. Each field has a one-line comment describing what it is.
3. Pass it to the reader with `schema='schema.yaml'`.

Do **not** commit your filled-in `schema.yaml` to a public repository — add it to `.gitignore`.

## Quick start

```python
from NielsenDSRS import NielsenIQRetail

reader = NielsenIQRetail(
    dir_read='/path/to/2006-2020/nielsen_extracts/RMS',     # Batch 1 (optional)
    dir_read_v2='/path/to/2021-Onward/nielsen_extracts/RMS', # Batch 2 (optional)
    schema='schema.yaml',                                    # your filled-in schema
)

# choose years (any mix across the two eras)
reader.filter_years(keep=[2018, 2023])

# stores (pandas); supports keep_dmas / keep_states / keep_channels
reader.read_stores()
reader.filter_stores()
reader.df_stores.head()

# products — Batch 1 -> df_products, Batch 2 -> df_products_b2
reader.read_products()

# 2021-onward product hierarchy and attributes
reader.read_hierarchy()
reader.read_attributes()

# sales — filter first, then read. df_sales is a lazy Dask DataFrame.
reader.filter_sales(keep_groups=[...], keep_modules=[...],          # Batch 1 selectors
                    keep_super_categories=[...])                    # Batch 2 selectors
reader.read_sales()
reader.df_sales.head()      # use .compute() to materialize, after filtering/aggregating
```

You can also use a single era alone — pass only `dir_read` (it auto-detects whether it is the 2006–2020 or 2021-onward format).

## Scaling with Dask

For a local in-memory client:
```python
from dask.distributed import Client

total_memory_gb = SYSTEM_RAM          # your system's total RAM in GB
n_workers = WORKERS                   # number of cores to use
memory_per_worker_gb = int(total_memory_gb / n_workers)

client = Client(n_workers=n_workers, threads_per_worker=1,
                memory_limit=f'{memory_per_worker_gb}GB')
print(client)
```

To use an auxiliary memory cluster for large data processing:
```python
from dask.distributed import Client
client = Client('dask-scheduler.default.svc.cluster.local:address')  # your cluster address
client
```

## Debug

Make sure the `NielsenDSRS` module and all dependencies are installed — with matching versions — on the Dask Client, Scheduler, and Worker nodes.

For worker nodes:
```python
def check_module():
    try:
        import NielsenDSRS
        return "Installed"
    except ImportError:
        return "Not Installed"

results = client.run(check_module)
for worker, result in results.items():
    print(f"{worker}: {result}")
```

For the scheduler:
```python
print("Scheduler:", client.run_on_scheduler(check_module))
```

If there is a mismatch or the package is not installed:
```python
def install_nielsendsrs():
    import subprocess
    subprocess.check_call(["pip", "install", "NielsenIQRetail"])

client.run(install_nielsendsrs)
client.run_on_scheduler(install_nielsendsrs)
```

## License

[MIT License](https://github.com/pratikrelekar/NielsenDSRS/blob/main/LICENSE)

## Background

This library was developed at [**Data Science Research Services (University of Illinois Urbana-Champaign)**](https://dsrs.illinois.edu) in 2024 and has been under active development since. It supports NielsenIQ Retail Scanner data from **2006 to the present** — the 2006–2020 format and the 2021-onward format — through a single unified reader (v1.0.0).

## Getting help

For general questions and discussions, visit the [**DSRS mailing list**](https://dsrs.illinois.edu/about/faq).
