package meetup.beeno;

import meetup.beeno.util.HUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 */
public class TestEntityIO {
  private static HBaseTestingUtility UTIL;
  private static Configuration conf;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    UTIL = new HBaseTestingUtility();
    UTIL.startMiniCluster();
    conf = UTIL.getConfiguration();
    HUtil.setPool(new HTablePool(conf, 5));
    // create a dummy HBase table for testing
    HBaseAdmin admin = new HBaseAdmin(conf);
    if (! admin.tableExists("test_simple")) {
      UTIL.createTable(Bytes.toBytes("test_simple"), Bytes.toBytes("props"));
    }
    if (! admin.tableExists("test_complex")) {
      HTableDescriptor complexDesc = new HTableDescriptor(Bytes.toBytes("test_complex"));
      HColumnDescriptor  propsCol = new HColumnDescriptor("props");
      propsCol.setMaxVersions(10);
      complexDesc.addFamily(propsCol);
      HColumnDescriptor extendedCol = new HColumnDescriptor("extended");
      extendedCol.setMaxVersions(10);
      complexDesc.addFamily(extendedCol);
      admin.createTable(complexDesc);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSaveAndGet() throws Exception {
    Entities.SimpleEntity entity1 = new Entities.SimpleEntity();
    entity1.setId("entity1");
    entity1.setStringProperty("all my words");
    entity1.setIntProperty(123);
    entity1.setFloatProperty(1.1f);
    entity1.setDoubleProperty(123.456789);
    entity1.setLongProperty(444444444444l);

    EntityService<Entities.SimpleEntity> service = new EntityService(Entities.SimpleEntity.class);
    service.save(entity1);

    Entities.SimpleEntity entity2 = service.get("entity1");
    assertNotNull(entity2);
    assertEquals(entity1.getId(), entity2.getId());
    assertEquals(entity1.getStringProperty(), entity2.getStringProperty());
    assertEquals(entity1.getIntProperty(), entity2.getIntProperty());
    assertEquals(entity1.getFloatProperty(), entity2.getFloatProperty(), 0.0001);
    assertEquals(entity1.getDoubleProperty(), entity2.getDoubleProperty(), 0.0001);
    assertEquals(entity1.getLongProperty(), entity2.getLongProperty());

    // update the retrieved entity and check for the changes
    entity2.setStringProperty("new stuff");
    entity2.setDoubleProperty(9.8765432101);
    entity2.setIntProperty(-17);
    service.save(entity2);

    Entities.SimpleEntity entity3 = service.get(entity2.getId());
    assertEquals("entity1", entity3.getId());
    assertEquals("new stuff", entity3.getStringProperty());
    assertEquals(-17, entity3.getIntProperty());
    assertEquals(1.1, entity3.getFloatProperty(), 0.0001);
    assertEquals(9.8765432101, entity3.getDoubleProperty(), 0.0001);
    assertEquals(444444444444l, entity3.getLongProperty());
  }

  /**
   * Test saving entities containing mapped collection properties
   * @throws Exception
   */
  @Test
  public void testSaveAndGetComplex() throws Exception {
    Entities.ComplexEntity entity1 = new Entities.ComplexEntity();
    entity1.setId("complex1");
    List strings = new ArrayList();
    strings.add("one");
    strings.add("two");
    entity1.setStringList(strings);
    Set ints = new HashSet();
    ints.add(1);
    ints.add(2);
    entity1.setIntSet(ints);
    Map extended = new HashMap();
    extended.put("prop1", "one");
    extended.put("prop2", "two");
    entity1.setExtendedProps(extended);

    EntityService<Entities.ComplexEntity> service = new EntityService(Entities.ComplexEntity.class);
    service.save(entity1);

    Entities.ComplexEntity entity2 = service.get("complex1");
    assertNotNull(entity2);
    assertEquals(entity1.getId(), entity2.getId());
    assertTrue(entity2.getStringList().contains("one"));
    assertTrue(entity2.getStringList().contains("two"));
    assertTrue(entity2.getIntSet().contains(new Long(1)));
    assertTrue(entity2.getIntSet().contains(new Long(2)));
    assertNotNull(entity2.getExtendedProps());
    assertEquals(entity2.getExtendedProps().get("prop1"), "one");
    assertEquals(entity2.getExtendedProps().get("prop2"), "two");
  }

  /**
   * Test saving multiple entities as a batch
   * @throws Exception
   */
  @Test
  public void testSaveMultiple() throws Exception {
    Entities.SimpleEntity[] entities = new Entities.SimpleEntity[]{
        new Entities.SimpleEntity("e1", "string1", 1, 1.1f, 1.1, 1),
        new Entities.SimpleEntity("e2", "string2", 2, 2.2f, 2.2, 2),
        new Entities.SimpleEntity("e3", "string3", 3, 3.3f, 3.3, 3),
        new Entities.SimpleEntity("e4", "string4", 4, 4.4f, 4.4, 4),
        new Entities.SimpleEntity("e5", "string5", 5, 5.5f, 5.5, 5),
        new Entities.SimpleEntity("e6", "string6", 6, 6.6f, 6.6, 6),
    };

    EntityService<Entities.SimpleEntity> srv = new EntityService(Entities.SimpleEntity.class);
    srv.saveAll(Arrays.asList(entities));

    List<Entities.SimpleEntity> saved = new ArrayList<Entities.SimpleEntity>(entities.length);
    for (Entities.SimpleEntity e : entities) {
        saved.add( srv.get(e.getId()) );
    }
    assertEquals(entities.length, saved.size());
    for (int cnt=0; cnt < entities.length; cnt++) {
      Entities.SimpleEntity savedEnt = saved.get(cnt);
      assertEquals(entities[cnt].getId(), savedEnt.getId());
      assertEquals(entities[cnt].getStringProperty(), savedEnt.getStringProperty());
      assertEquals(entities[cnt].getIntProperty(), savedEnt.getIntProperty());
      assertEquals(entities[cnt].getFloatProperty(), savedEnt.getFloatProperty(), 0.0001);
      assertEquals(entities[cnt].getDoubleProperty(), savedEnt.getDoubleProperty(), 0.0001);
      assertEquals(entities[cnt].getLongProperty(), savedEnt.getLongProperty());
    }
  }

  /**
   * Test timeouts when saving recs
   * @throws Exception
   */
  /* Explicit timeout not implemented ??
  public void testSaveTimeout() throws Exception {
    Entities.SimpleEntity newent = new Entities.SimpleEntity("ne1", "string one", 1, 1.1f, 1.1, 1);
    EntityService<Entities.SimpleEntity> service = new EntityService(Entities.SimpleEntity.class);
    try {
      // save with too short timeout
      boolean success = service.save(newent, 1);
      assertFalse("Expected save request to timeout", success);
    }
    catch (HBaseException e) {
      fail("Save failed with unexpected exception");
    }
  }
  */
}
