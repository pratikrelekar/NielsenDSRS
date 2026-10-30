from setuptools import setup, find_packages

setup(
    name="NielsenIQRetail",
    version="1.0.0",
    packages=find_packages(exclude=["tests*"]),
    include_package_data=True,
    package_data={"NielsenDSRS": ["schema.example.yaml"]},
    install_requires=[
        "dask[dataframe]==2024.1.1",
        "distributed==2024.1.1",
        "numpy==1.26.3",
        "pandas==2.2.0",
        "toolz==0.12.0",
        "pyyaml>=6.0",
    ],
    author="Pratik Relekar, Hrishikesh Relekar, Matias Carrasco Kind",
    author_email="pratik.relekar@gmail.com, hrish.relekar@gmail.com, mcarras2@illinois.edu",
    description="NielsenIQ Reader — processes NielsenIQ Retail Scanner data (2006-2020 and 2021-onward)",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/pratikrelekar/NielsenDSRS",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
