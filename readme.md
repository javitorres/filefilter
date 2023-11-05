<H1>FileFilter</H1>

<p align="center">
    <img src="https://img.shields.io/badge/Version-0.1.0-red" alt="Latest Release">
    <img src="https://img.shields.io/badge/DuckDB-0.9.1-yellow" alt="DuckDB">

</p>


# FileFilter
FileFilter is a ETL tool to load, transform and save data files. Transformations are defined in a YAML file.

The YAML configuration files includes settings for the delimiter used in the output file, the number of filter threads, the number of sample lines to process, and how often to reload the configuration. It also specifies three filters with their configurations, which will be applied to the data in sequence: a REST filter to make an HTTP request and add the response to the data, a Python filter to execute some Python code, and an SQL filter to execute a SQL query.

Filter types:

* **restFilter**: This function takes a row of data and an actionConfig dictionary. It then makes an HTTP request using parameters extracted from the actionConfig and the data row. The HTTP response is added to the row as a new column if the status is 200.

* **pythonFilter**: This function compiles and executes Python code specified in the actionConfig with the current row's data. The result of this execution (if any) is expected to modify the row's data.

* **sqlFilter**: This function executes SQL code provided in the actionConfig using duckdb on the dataframe df. The result of this query replaces the current dataframe.



# Configuration

Install dependencies listed in requirements.txt:

```
pip install -r requirements.txt
```

# Example
usage: python3 filefilter.py FILE_IN FILTERS.yml FILE_OUT 
