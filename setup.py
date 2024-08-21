from setuptools import setup, find_packages

setup(
    name="NielsenIQRetail",
    version="0.2.4",  # Update this with your new version
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
    description="NielsenIQ Reader’s main purpose is to facilitate ease of processing of NielsenIQ Retail Scanner data",
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
