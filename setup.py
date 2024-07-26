from setuptools import setup, find_packages

setup(
    name="NielsenRetail",
    version="0.1.0",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "dask[dataframe]==2024.1.1",
        "distributed==2024.1.1",
        "numpy==1.26.3",
        "pandas==2.2.0",
        "toolz==0.12.0"
    ],
    author="Pratik Relekar, Hrishikesh Relekar, Matias Carrasco Kind",
    author_email="relekar2@illinois.edu, hrishkesh.relekar@chicagobooth.edu, mcarras2@illinois.edu",
    description="Nielsen Reader’s main purpose is to facilitate ease of processing of Nielsen Retail Scanner data of Kilt’s Center’s Nielsen IQ data used for Academic research only. The striking feature of this library is Dask which acts as an underlying framework that uniquely empowers the user to read Nielsen data with limited on device resources (by processing larger-than-memory data in chunks and parallel fashion). It understands the Kilts/Nielsen directory structure.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/pratikrelekar/NielsenDSRS",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
