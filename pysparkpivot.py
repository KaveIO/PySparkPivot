##############################################################################
#
# Copyright 2016 KPMG LLP UK
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
##############################################################################
"""
The pysparkpivot module adds functionality to Spark to manipulate RDDs or PySpark DataFrames into pivot tables

You need Pyspark installed and on the pythonpath

PivotTableFactory is the main operating class in this library,
of which the create() method enables the creation of a pivot table from a SchemaRDD or DataFrame.

Example usage
--------------

>>> import pandas as pd
>>> pandas_df = pd.read_csv('../examples/iris.csv')
>>> spark_df = sqlContext.createDataFrame(pandas_df)
>>> from pysparkpivot import PivotTableFactory
>>> piv = PivotTableFactory(spark_df, sqlContext)
>>> pivot_table = piv.create(index=['Target'], columns=['Colour'],
...                     values=['SepLength'], aggregates=['AVG'])
>>> pivot_table.show()

Note only one type of aggregation can be performed at one time

The DataFrameFactory class can also be used to generate a PySpark DataFrame from an RDD.

"""

import pyspark.sql as _pssql
import time as _time
import itertools as _it

__all__=["PivotTableFactory", "DataFrameFactory"]


class PivotTableFactory(object):
    '''
    Creates a spreadsheet-style as a PySpark DataFrame from a SchemaRDD or PySpark DataFrame

    Requires SQL Context: pyspark.sql.SQLContext(sparkContext, sqlContext=None)
    '''
    def _enum(**enums):
        return type('Enum', (), enums)

    PivotFunction = _enum(AVG='AVG', MAX='MAX', SUM='SUM', COUNT='COUNT',
                          MIN='MIN')

    def __init__(self, rdd, sqlCtx):
        if not isinstance(rdd, _pssql.DataFrame):
            raise Exception("PivotTable requires a DataFrame, "
                            "please create one using DataFrameFactory")
        self.rdd = rdd
        self.sqlCtx = sqlCtx

    def create(self, index, columns, values, aggregates):
        """
        Creates a PivotTable from a SchemaRDD or DataFrame

        Parameters:
        -----------
        :param index: a column or list of columns. Keys to group by on the pivot table index.
        :param columns: a column  or list of them. Keys to group by on the pivot table column.
        :param values: columns to aggregate.
        :param aggregates: function with which to aggregate. Options: 'AVG','MAX','SUM','COUNT','MIN'.
        Note only one type of aggregation can be performed at one time

        Example usage:
        --------------

        >>> import pandas as pd
        >>> pandas_df = pd.read_csv('./examples/iris.csv')
        >>> spark_df = sqlContext.createDataFrame(pandas_df)
        >>> from PySparkPivot import PivotTableFactory
        >>> piv = PivotTableFactory(spark_df, sqlContext)
        >>> pivot_table = piv.create(index=['Target'], columns=['Colour'],
        ...                     values=['SepLength'], aggregates=['AVG'])
        >>> pivot_table.show()

        Returns:
        ---------
        :return table: PySpark DataFrame

        """
        table = PivotTable(index, columns, values, aggregates,
                           self.rdd, self.sqlCtx)
        return self.sqlCtx.createDataFrame(table.transform(), table.schema())


class PivotTable(object):
    '''
    Principal class for the generation of a pivot table similar to that of the pandas library
    See http://pandas.pydata.org/pandas-docs/stable/generated/pandas.pivot_table.html.
    Called by PivotTableFactory
    '''

    def __init__(self, index, columns, values, aggregates, rdd, sqlCtx):
        self.index = index
        self.columns = columns
        self.values = values
        self.aggregates = aggregates
        self.rdd = rdd
        self.sqlCtx = sqlCtx
        self.metrics = self._metrics()
        self.groupeddata = self._groupedData(rdd)
        self.header = self._header()

    def _header(self):
        h = PivotHeader(self.groupeddata, self.columns, self.sqlCtx)
        return h.toHeader()

    def _groupedData(self, rdd):
        pg = PivotGroup(self)
        return pg.toRDD()

    def _metrics(self):
        metrics = _it.izip_longest(self.aggregates, self.values,
                                   fillvalue=self.aggregates[-1])
        metrics_list = [PivotMetric(field, aggregate)
                        for (aggregate, field) in metrics]
        return metrics_list

    def transform(self):
        pa = PivotAction(self.index, self.columns, self.header, self.metrics)
        return pa.pivot(self.groupeddata)

    def schema(self):
        ps = PivotSchema(self.index, self.metrics, self.header)
        return ps.toSchema()


class PivotAction(object):
    '''
    Called by PivotTable
    '''
    def __init__(self, index, columns, header, metrics):
        self.cc = PivotColumnClosure(index)
        self.rc = PivotRowClosure(index, columns, header, metrics)

    def pivot(self, groupeddata):
        stats = self.cc.pivotcolumn(groupeddata)
        return self.rc.pivotrow(stats)


class PivotSchema(object):
    '''
    Called by PivotTable
    '''
    def __init__(self, index, metrics, header):
        self.index = index
        self.metrics = metrics
        self.header = header

    def _labelsForMetric(self, metricname):
        return ['%s_%s' % (metricname, label) for label in self.header]

    def _buildFieldsFrom(self, fields, fieldtype=_pssql.types.StringType(), nullable=False):
        return [_pssql.types.StructField(field, fieldtype, nullable) for field in fields]

    def toSchema(self):
        fields = self._buildFieldsFrom(self.index)
        for metric in self.metrics:
            fields += self._buildFieldsFrom(self._labelsForMetric(metric.name),
                                            _pssql.types.DoubleType(), True)
        return _pssql.types.StructType(fields)


class PivotHeader(object):
    '''
    Called by PivotTable
    '''
    def __init__(self, rdd, columns, sqlCtx):
        self.tablename = "mtable_" + str(int(round(_time.time())))
        rdd.registerAsTable(self.tablename)
        self.sqlTemplate = "SELECT DISTINCT(%s) FROM %s"
        self.columns = columns
        self.sqlCtx = sqlCtx

    def _labelsFor(self, field):
        # TODO: possible problem for datasets with high cardinality
        sql = self.sqlTemplate % (field, self.tablename)
        labels = self.sqlCtx.sql(sql)
        return [getattr(row, field) for row in labels.collect()]

    def toHeader(self):
        all_labels = [self._labelsFor(field) for field in self.columns]
        all_keys = list(_it.product(*all_labels))
        return ['_'.join(key) for key in all_keys]


class PivotGroup(object):
    '''
    Called by PivotTable
    '''
    def __init__(self, table):
        self.table = table
        self.tablename = "ptable_" + str(int(round(_time.time())))
        self.sep = ', '

    def _sql(self):
        sql = 'SELECT '
        if len(self.table.index) > 0:
            sql += self._columnify(self.table.index)
            sql += self.sep
        if len(self.table.columns) > 0:
            sql += self._columnify(self.table.columns)
            sql += self.sep
        sql += self._columnify(self.table.metrics)
        sql += ' FROM '
        sql += self.tablename
        sql += ' GROUP BY '
        if len(self.table.index) > 0:
            sql += self._columnify(self.table.index)
        if len(self.table.columns) > 0:
            sql += self.sep
            sql += self._columnify(self.table.columns)
        print "PIVOT SQL: %s" % (sql,)
        return sql

    def _columnify(self, columns):
        return self.sep.join(str(c) for c in columns)

    def toRDD(self):
        self.table.rdd.registerAsTable(self.tablename)
        return self.table.sqlCtx.sql(self._sql())


class PivotColumnClosure(object):
    '''
    Called by PivotAction
    '''
    def __init__(self, index):
        self.index = index

    def pivotcolumn(self, rdd):
        return rdd.map(lambda row: self._pivotcolumn(row)).groupByKey()

    def _columns(self, row):
        return [getattr(row, attr) for attr in self.index]

    def _pivotcolumn(self, row):
        key = tuple(self._columns(row))
        return (key, row)


class PivotRowClosure(object):
    '''
    Called by PivotAction
    '''
    def __init__(self, index, columns, labels, metrics):
        self.index = index
        self.columns = columns
        self.labels = labels
        self.metrics = metrics

    def pivotrow(self, rdd):
        return rdd.map(
            lambda key_rows: self._pivotrow(
                key_rows[0], key_rows[1]))

    def _pivotrow(self, key, rows):
        result = list(key)
        rowMap = self._twistByColumns(rows)
        for metric in self.metrics:
            result += self._enumerateValuesForMetric(metric, rowMap)
        return tuple(result)

    def _twistByColumns(self, rows):
        rowMap = dict()
        for row in rows:
            key = '_'.join([getattr(row, field) for field in self.columns])
            rowMap[key] = row
        return rowMap

    def _enumerateValuesForMetric(self, metric, rowMap):
        values = []
        for label in self.labels:
            val = None
            if label in rowMap:
                row = rowMap[label]
                val = getattr(row, metric.name, None)
            values.append(val)
        return values


class PivotMetric(object):
    '''
    Class to define SQL statement for given metric
    Called by PivotTable
    '''
    def __init__(self, field, metric='AVG'):
        self.field = field
        self.metric = metric
        self.name = metric + '_' + field
        self.sql = self._tosql()

    def __str__(self):
        return self.sql

    def _tosql(self):
        sql = ''
        sql += self.metric
        sql += '(CAST('
        sql += self.field
        sql += ' AS DOUBLE)) AS '
        sql += self.name
        return sql


class DataFrameFactory(object):
    '''
    Class can be used to generate a dataframe from an RDD
    Parameters:
    -----------
    columns: names of the headers of each column
    separator: delimiter to use, default ','
    rdd: the data Spark RDD

    Returns:
    Spark DataFrame
    '''

    def __init__(self, columns, sqlCtx, separator=','):
        self.sqlCtx = sqlCtx
        self.parser = RowParser(columns, separator)

    def create(self, rdd):
        '''
        Create the PySpark DataFrame from an RDD
        :param rdd: RDD for convertsion
        :return table: DataFrame
        '''
        return self.sqlCtx.createDataFrame(self.parser.parse(rdd))


class RowParser(object):
    '''
    Parses the rows of an RDD using relevant headers and separator.
    Called by DataFrameFactory
    '''

    def __init__(self, header, separator):
        self.sep = separator
        self.header = header

    def parse(self, rdd):
        return rdd.map(lambda line: self._parserow(line))

    def _parserow(self, line):
        d = dict(zip(self.header, line.split(self.sep)))
        return _pssql.types.Row(**d)
