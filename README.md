# PySparkPivot

PySparkPivot is a small python module for Spark, to manipulate PySpark Dataframes/RDD into spreadsheet-style pivot tables for data aggregation.

## Introduction

We developed this module to provide pivot table functionality for PySpark. The concept is very similar to that in the
Pandas library - see http://pandas.pydata.org/pandas-docs/stable/generated/pandas.pivot_table.html.
Data is often stored in CSV files or databases in so-called “stacked” or “record” format. For some analyses it's
 preferable for the columns and indices of a table to represent unique values rather than individual observations.
Pivot tables allow you to reshape the data in this way, aggregating over individual observations using a variety of functions.

The simplest form of pivot table is a cross-tabulation, counting the number of observations matching the values of the index and column.
This functionality is already available in the crosstab function in the pyspark.sql.DataFrame class Crosstab:
 https://spark.apache.org/docs/1.5.2/api/python/pyspark.sql.html#pyspark.sql.DataFrame. Crosstab computes a pair-wise
 frequency (contingency) table of the given columns.

 This module allows for the added aggregation functions of average (mean), max, min and sum.


## Requirements

 - Spark and PySpark need to be installed and on your path.

 - Pandas if you want to run the examples

You will also need to have a Spark context and SQL context running.

## Installation

 - Download the zip and extract contents

- Either run your jobs from this directory, set this on your PYTHONPATH, or copy the module to somewhere already on your PYTHONPATH

## Examples
Generate a pivot table for an edited version of the Iris data set, which has an extra randomised colour variable.

```
>>> # see the inline help
>>> import pysparkpivot
>>> help(pysparkpivot)
>>> # Loading a small csv into spark and then pivoting it
>>> import pandas as pd
>>> pandas_df = pd.read_csv('./examples/iris.csv')
>>> spark_df = sqlContext.createDataFrame(pandas_df)
>>> from pysparkpivot import PivotTableFactory
>>> piv = PivotTableFactory(spark_df, sqlContext)
>>> pivot_table = piv.create(index=['Target'], columns=['Colour'],
...                     values=['SepLength'], aggregates=['AVG'])
>>> pivot_table.show()
```

Also see the Jupyter notebook "Pivot-demo" in the examples folder

## Plans for this module

Our plan is to make this into a Spark package so that it can be imported into Spark directly. We would ideally like to add
 further aggregation functions such as median. Pull requests welcome.
