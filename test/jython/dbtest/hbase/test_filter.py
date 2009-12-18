#
# Test comparison operations for custom HBase filters
#
from meetup.beeno.filter import *
from meetup.beeno.util import *

from org.apache.hadoop.hbase import KeyValue
from org.apache.hadoop.hbase.util import Bytes

import java.lang
from java.util import Locale

from jyunit.util import *


def test_column_eq():
    '''Checks column equals operation in ColumnMatchFilter'''
    fam1 = java.lang.String('fam1')
    col1 = java.lang.String('col1')
    col2 = java.lang.String('col2')
    fam1col1 = java.lang.String.format("%s:%s", [fam1, col1])
    colval = java.lang.String('myvalue')
    mismatchval = java.lang.String('secondvalue')
    rowkey1 = Bytes.toBytes( java.lang.String('row1') )

    colfilt = ColumnMatchFilter(Bytes.toBytes(fam1col1), ColumnMatchFilter.CompareOp.EQUAL, PBUtil.toBytes(colval), True)

    row1 = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(colval))
    row_ne = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(mismatchval))
    row_missing = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col2), PBUtil.toBytes(colval))

    colfilt.filterKeyValue(row1)
    assertFalse( colfilt.filterRow(), "Row with matching value should not be filtered" )
    colfilt.reset()

    colfilt.filterKeyValue(row_ne)
    assertTrue( colfilt.filterRow(), "Row with mismatched value should be filtered" )
    colfilt.reset()

    colfilt.filterKeyValue(row_missing)
    assertTrue( colfilt.filterRow(), "Row missing column should be filtered" )
    colfilt.reset()

    # test row missing column without 'filter if missing' flag
    colfilt = ColumnMatchFilter(Bytes.toBytes(fam1col1), ColumnMatchFilter.CompareOp.EQUAL, PBUtil.toBytes(colval), False)

    colfilt.filterKeyValue(row_missing)
    assertFalse( colfilt.filterRow(), "Row missing column should not be filtered without flag" )
    colfilt.reset()



def test_column_ne():
    '''Checks column not equal operation in ColumnMatchFilter'''
    fam1 = java.lang.String('fam1')
    col1 = java.lang.String('col1')
    col2 = java.lang.String('col2')
    fam1col1 = java.lang.String.format("%s:%s", [fam1, col1])
    colval = java.lang.String('myvalue')
    mismatchval = java.lang.String('secondvalue')
    rowkey1 = Bytes.toBytes( java.lang.String('row1') )

    colfilt = ColumnMatchFilter(Bytes.toBytes(fam1col1), ColumnMatchFilter.CompareOp.NOT_EQUAL, PBUtil.toBytes(colval), True)

    row1 = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(colval))
    row_ne = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(mismatchval))
    row_missing = KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col2), PBUtil.toBytes(colval))

    colfilt.filterKeyValue(row1)
    assertTrue( colfilt.filterRow(), "Row with matching value should be filtered" )
    colfilt.reset()

    colfilt.filterKeyValue(row_ne)
    assertFalse( colfilt.filterRow(), "Row with mismatched value should not be filtered" )
    colfilt.reset()

    colfilt.filterKeyValue(row_missing)
    assertTrue( colfilt.filterRow(), "Row missing column should be filtered" )
    colfilt.reset()

    # test row missing column without 'filter if missing' flag
    colfilt = ColumnMatchFilter(Bytes.toBytes(fam1col1), ColumnMatchFilter.CompareOp.NOT_EQUAL, PBUtil.toBytes(colval), False)

    colfilt.filterKeyValue(row_missing)
    assertFalse( colfilt.filterRow(), "Row missing column should not be filtered without flag" )
    colfilt.reset()



def run_test():
    test_column_eq()
    test_column_ne()

if __name__ == "__main__":
	run_test()
