package meetup.beeno.filter;

import meetup.beeno.util.PBUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyBoolean;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class TestColumnMatchFilter {
  private String fam1 = "fam1";
  private String col1 = "col1";
  private String col2 = "col2";
  private String colval = "myvalue";
  private String mismatchval = "secondvalue";
  private byte[] rowkey1 = Bytes.toBytes("row1");

  /**
   * Checks column equals operation in ColumnMatchFilter
   */
  @Test
  public void testColumnEqual() {

    ColumnMatchFilter colfilt = new ColumnMatchFilter(Bytes.toBytes(fam1), Bytes.toBytes(col1), ColumnMatchFilter.CompareOp.EQUAL, PBUtil.toBytes(colval), true);

    KeyValue row1 = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(colval));
    KeyValue row_ne = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(mismatchval));
    KeyValue row_missing = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col2), PBUtil.toBytes(colval));

    colfilt.filterKeyValue(row1);
    assertFalse("Row with matching value should not be filtered", colfilt.filterRow());
    colfilt.reset();

    colfilt.filterKeyValue(row_ne);
    assertTrue("Row with mismatched value should be filtered", colfilt.filterRow());
    colfilt.reset();

    colfilt.filterKeyValue(row_missing);
    assertTrue("Row missing column should be filtered", colfilt.filterRow());
    colfilt.reset();

    // test row missing column without 'filter if missing' flag
    colfilt = new ColumnMatchFilter(Bytes.toBytes(fam1), Bytes.toBytes(col1), ColumnMatchFilter.CompareOp.EQUAL, PBUtil.toBytes(colval), false);

    colfilt.filterKeyValue(row_missing);
    assertFalse("Row missing column should not be filtered without flag", colfilt.filterRow());
    colfilt.reset();
  }

  /**
   * Checks column not equal operation in ColumnMatchFilter
   */
  public void testColumnNotEqual() {
    ColumnMatchFilter colfilt = new ColumnMatchFilter(Bytes.toBytes(fam1), Bytes.toBytes(col1), ColumnMatchFilter.CompareOp.NOT_EQUAL, PBUtil.toBytes(colval), true);

    KeyValue row1 = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(colval));
    KeyValue row_ne = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col1), PBUtil.toBytes(mismatchval));
    KeyValue row_missing = new KeyValue(rowkey1, Bytes.toBytes(fam1), Bytes.toBytes(col2), PBUtil.toBytes(colval));

    colfilt.filterKeyValue(row1);
    assertTrue("Row with matching value should be filtered", colfilt.filterRow());
    colfilt.reset();

    colfilt.filterKeyValue(row_ne);
    assertFalse("Row with mismatched value should not be filtered", colfilt.filterRow());
    colfilt.reset();

    colfilt.filterKeyValue(row_missing);
    assertTrue("Row missing column should be filtered", colfilt.filterRow());
    colfilt.reset();

    // test row missing column without 'filter if missing' flag
    colfilt = new ColumnMatchFilter(Bytes.toBytes(fam1), Bytes.toBytes(col1), ColumnMatchFilter.CompareOp.NOT_EQUAL, PBUtil.toBytes(colval), false);

    colfilt.filterKeyValue(row_missing);
    assertFalse("Row missing column should not be filtered without flag", colfilt.filterRow());
    colfilt.reset();
  }
}
