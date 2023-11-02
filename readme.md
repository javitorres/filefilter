<H1>FileFilter</H1>

<p align="center">
    <img src="https://img.shields.io/badge/Version-0.1.0-red" alt="Latest Release">
    <img src="https://img.shields.io/badge/DuckDB-0.9.1-yellow" alt="DuckDB">

</p>


# FileFilter
FileFilter is a ETL tool to load, transform and save data files.

# Configuration

Install dependencies listed in requirements.txt:

```
pip install -r requirements.txt
```

# Example
usage: python3 filefilter.py FILE_IN FILTERS.yml FILE_OUT 

# Available filters

## Rest

Use external APIs to get data using info from the input file

## Python

Write transform operations in Python to combine data and columns from input file