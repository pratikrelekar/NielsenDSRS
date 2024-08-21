# NielsenIQ Retail Reader


| | |
| --- | --- |
| License | [![License](https://img.shields.io/badge/LICENSE-blue)](https://github.com/pratikrelekar/NielsenDSRS/blob/main/LICENSE) |
| Dependencies | [![PyPI - Version](https://img.shields.io/pypi/v/pandas?logo=pandas&logoColor=blue&label=Pandas&color=blue)](https://pandas.pydata.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/dask?logo=Dask&label=Dask&color=orange)](https://www.dask.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/distributed?logo=dask&logoColor=yellow&label=Distributed&color=yellow)](https://distributed.dask.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/numpy?logo=numpy&logoColor=green&label=Numpy&color=green)](https://numpy.org) \| [![PyPI - Version](https://img.shields.io/pypi/v/toolz?logo=toolz&logoColor=red&label=Toolz&color=red)](https://pypi.org/project/toolz/)
| Meta | [![PyPI - Version](https://img.shields.io/pypi/v/NielsenRetail?label=PyPI&color=orange)](https://pypi.org/project/NielsenRetail/)




## Overview:

**NielsenIQ Retail Reader** is a special-purpose library and it's main purpose is to facilitate ease of processing of NielsenIQ Retail Scanner data of Kilt’s Center’s Nielsen IQ data used for Academic research only. The striking feature of this library is Dask which acts as an underlying framework that uniquely empowers the user to read NielsenIQ data with limited on device resources (by processing larger-than-memory data in chunks and distributed fashion). It understands the Kilts/Nielsen directory structure.

## Data:
Information about the Retail Scanner data can be found here: [**Kilts Center for Marketing**](https://www.chicagobooth.edu/research/kilts/research-data/nielseniq)

## IMPORTANT:

### Access to NielsenIQ Retail Data:

Please note that NielsenIQ Retail data is proprietary and access is restricted to individuals whose institutions have an existing subscription or agreement with NielsenIQ. If you intend to use this library for accessing and analyzing NielsenIQ data, you must first ensure that you are authorized to do so by your institution. Unauthorized access or use of this data may violate terms of use and could have legal implications. Nielsen dataset should strictly follow standard naming convention as per laid out by Nielsen and Kilts Center of Marketing and under no circumstances the naming convention should be changed.

**NielsenIQRetail** processes Retail Scanner Data.

## Table of Contents

- [Main Features](#main-features)
- [Where to get it](#where-to-get-it)
- [Dependencies](#dependencies)
- [How to use](#how-to-use)
- [Debug](#debug)
- [License](#license)
- [Background](#background)
- [Getting Help](#getting-help)

## Main Features
Here are just a few of the things that NielsenIQRetail does well:
  - Efficiently manages NielsenIQ directory and hierarchy, simplifying the process for researchers and significantly reducing the time needed to navigate through NielsenIQ documentation.
  - Size mutability: Processes dataframes [**larger-than-memory**](https://examples.dask.org/dataframe.html) on a single machine through batch processing.
  - Distributed computing for terabyte sized datasets enhancing the overall data reading speed by utlising [**low-latency**](https://distributed.dask.org/en/stable/) feature of Dask.
  - Provides simple yet distinct commands for separating sales, stores, and products data for analysis purposes.
  - This package has excellent compatibility with [**Numpy**](https://numpy.org), [**Pandas**](https://pandas.pydata.org) etc.


## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/pratikrelekar/NielsenIQDSRS

Binary installers is available at [Python Package Index (PyPI)](https://pypi.org/)

For PyPI install:
```sh
pip install NielsenIQRetail
```


For Github pip install:
```sh
pip install git+https://github.com/pratikrelekar/NielsenIQDSRS
```

For pip install requirements:
```sh
python -m pip install -r requirements.txt
```


## Dependencies

Before using NielsenIQRetail, ensure that all dependencies are correctly installed. Additionally, verify that the Client hosting the Python environment, the Scheduler, and the Worker nodes all have the same version installed.

- [NumPy - Adds support for large, multi-dimensional arrays, matrices and high-level mathematical functions to operate on these arrays](https://pypi.org/project/numpy/1.26.3/)
- [Pandas - Provides high-performance, easy-to-use data structures, and data analysis tools.](https://pypi.org/project/pandas/2.2.0/)
- [Dask - Flexible parallel computing library for analytic computing, enabling performance at scale for the tools of your choice](https://pypi.org/project/dask/2024.1.1/)
- [Dask Distributed - Enables parallel computing and scaling to clusters for large computations, enhancing Dask’s capabilities to work across multiple machines by distributing tasks and managing workloads efficiently](https://pypi.org/project/distributed/2024.1.1/)
- [Toolz - Provides functional utilities for working with iterable data, enabling more efficient and readable data processing by offering a set of pure functions inspired by constructs from functional programming](https://pypi.org/project/toolz/0.12.0/)



## How to use

For Local on system memory:

```sh
from dask.distributed import Client

# # Calculate memory per worker based on total system memory
total_memory_gb = SYSTEM_RAM  # Your system's total RAM in GB (Edit as per system memory)
n_workers = WORKERS         # Number of cores on system (Edit the total workers you want)
memory_per_worker_gb = int(total_memory_gb / n_workers)  # Memory per worker in GB

# Start the client with given specifications
client = Client(n_workers=n_workers, threads_per_worker=1, 
                memory_limit=f'{memory_per_worker_gb}GB')
print(client)
```


To utilize the power of the Dask, using auxilary memory cluster for large data processing
```sh
# you can only connect to the cluster from inside Python client environment
from dask.distributed import Client
client = Client('dask-scheduler.default.svc.cluster.local:address') #Replace the address with your actual address of the memory cluster
client
```


## Debug

Make sure the NielsenDSRS module and all the dependencies are installed on Dask Client, Scheduler and Worker nodes. The versions should match on all. Following is the code to debug the errors related to the version mismatch:

For worker nodes:
```sh
def check_module():
    try:
        import NielsenIQDSRS
        return "Installed"
    except ImportError:
        return "Not Installed"

# Run the check across all workers
results = client.run(check_module)
for worker, result in results.items():
    print(f"{worker}: {result}")
```

For Scheduler:
```sh
scheduler_result = client.run_on_scheduler(check_module)
print(f"Scheduler: {scheduler_result}")
```

For Client:
```sh
try:
    import NielsenIQDSRS
    print("NielsenIQDSRS is installed on the client.")
except ImportError:
    print("NielsenIQDSRS is not installed on the client.")
```


If there is a mismatch or if the NielsenIQRetail is not correctly installed, follow these steps:


```sh
# Function to install NielsenIQDSRS
def install_nielseniqdsrs():
    import subprocess
    subprocess.check_call(["pip", "install", "NielsenIQRetail"])

# Install on all workers
c.run(install_nielseniqdsrs)

# Install on the scheduler
c.run_on_scheduler(install_nielseniqdsrs)
```




## License

[MIT License](https://github.com/pratikrelekar/NielsenIQDSRS/blob/main/LICENSE)


## Background

This library was developed at [**Data Science Research Services(University of Illinois at Urbana-Champaign)**](https://dsrs.illinois.edu) in 2024 and has been under active development since then. Currently supports NielsenIQ Retail Scanner data from 2006 to 2020.


## Getting help

For general questions and discussions, visit [**DSRS mailing list**](https://dsrs.illinois.edu/about/faq).


