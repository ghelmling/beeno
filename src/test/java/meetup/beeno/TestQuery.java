package meetup.beeno;

import meetup.beeno.util.HUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 */
public class TestQuery {
  private static HBaseTestingUtility UTIL;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    HUtil.setPool(new HTablePool(UTIL.getConfiguration(), 5));
    // create a dummy HBase table for testing
    UTIL.createTable(Bytes.toBytes("test_indexed"), Bytes.toBytes("props"));
    UTIL.createTable(Bytes.toBytes("test_indexed-by_intcol"), Bytes.toByteArrays(new String[]{"props", "__idx__"}));
    UTIL.createTable(Bytes.toBytes("test_indexed-by_stringcol"), Bytes.toByteArrays(new String[]{"props", "__idx__"}));

    EntityService<Entities.IndexedEntity> srv = new EntityService(Entities.IndexedEntity.class);
    long now = java.lang.System.currentTimeMillis();

    srv.save( new Entities.IndexedEntity("e1", "duck", 1, now - 100) );
    srv.save( new Entities.IndexedEntity("e2", "duck", 2, now - 80) );
    srv.save( new Entities.IndexedEntity("e3", "duck", 2, now - 60) );
    srv.save( new Entities.IndexedEntity("e4", "goose", 2, now - 40) );
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void queryByString() throws Exception {
    EntityService<Entities.IndexedEntity> srv = new EntityService(Entities.IndexedEntity.class);
    // test indexing of a value with multiple entries
    Query<Entities.IndexedEntity> q = srv.query();
    q.using( Criteria.eq( "stringProperty", "duck" ) );
    List<Entities.IndexedEntity> matches = q.execute();

    assertEquals( 3, matches.size() );
    assertEquals("e1", matches.get(0).getId());
    assertEquals("duck", matches.get(0).getStringProperty());
    assertEquals("e2", matches.get(1).getId());
    assertEquals("duck", matches.get(1).getStringProperty());
    assertEquals("e3", matches.get(2).getId());
    assertEquals("duck",  matches.get(2).getStringProperty());

    q = srv.query();
    q.using( Criteria.eq( "stringProperty", "goose" ) );
    matches = q.execute();
    assertEquals( 1, matches.size() );
    assertEquals("e4", matches.get(0).getId());
    assertEquals("goose", matches.get(0).getStringProperty());
  }

  @Test
  public void queryByInt() throws Exception {
    EntityService<Entities.IndexedEntity> srv = new EntityService(Entities.IndexedEntity.class);
    // test indexing of integer values
    Query<Entities.IndexedEntity> q = srv.query();
    q.using( Criteria.eq( "intKey", 2 ) );
    List<Entities.IndexedEntity> matches = q.execute();

    assertEquals( 3, matches.size() );
    assertEquals("Indexed entries should be in reverse timestamp order", "e4", matches.get(0).getId());
    assertEquals(new Integer(2), matches.get(0).getIntKey());
    assertEquals("Indexed entries should be in reverse timestamp order", "e3",  matches.get(1).getId());
    assertEquals(new Integer(2), matches.get(1).getIntKey());
    assertEquals("Indexed entries should be in reverse timestamp order", "e2",  matches.get(2).getId());
    assertEquals(new Integer(2), matches.get(2).getIntKey());

    q = srv.query();
    q.using( Criteria.eq( "intKey", 1 ) );
    matches = q.execute();
    assertEquals( 1, matches.size() );
    assertEquals("e1", matches.get(0).getId());
    assertEquals(new Integer(1), matches.get(0).getIntKey());
  }
}
