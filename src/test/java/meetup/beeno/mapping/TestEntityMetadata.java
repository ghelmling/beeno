package meetup.beeno.mapping;

import meetup.beeno.Entities;
import org.junit.Test;

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 */
public class TestEntityMetadata {

  private void assertEntity(Class entitycls, String tableName, Map<ColumnQualifier, String> fieldProps) throws Exception {
		EntityMetadata metadata = EntityMetadata.getInstance();
		EntityInfo info = metadata.getInfo(entitycls);

		assertEquals(tableName, info.getTablename());

		PropertyDescriptor keyprop = info.getKeyProperty();
		assertNotNull(keyprop);
		assertEquals("id", keyprop.getName());

		for (Map.Entry<ColumnQualifier,String> e : fieldProps.entrySet()) {
			PropertyDescriptor prop = info.getFieldProperty(e.getKey());
			assertNotNull(prop);
			assertEquals(e.getValue(), prop.getName());
    }
  }


  @Test
  public void testParsing() throws Exception {
    // check the entity mapping test cases
    // simple success case
    Map<ColumnQualifier,String> expectedFields = new HashMap<ColumnQualifier,String>();
    expectedFields.put(new ColumnQualifier("props", "stringcol"), "stringProperty");
    expectedFields.put(new ColumnQualifier("props", "intcol"), "intProperty");
    expectedFields.put(new ColumnQualifier("props", "floatcol"), "floatProperty");
    expectedFields.put(new ColumnQualifier("props", "doublecol"), "doubleProperty");
    expectedFields.put(new ColumnQualifier("props", "longcol"), "longProperty");
    assertEntity(Entities.SimpleEntity.class, "test_simple", expectedFields);

    // check basic field mapping when indexes are present ########
    expectedFields = new HashMap<ColumnQualifier, String>();
    expectedFields.put(new ColumnQualifier("props", "stringcol"), "stringProperty");
    expectedFields.put(new ColumnQualifier("props", "tscol"), "timestamp");
    assertEntity(Entities.IndexedEntity.class, "test_indexed", expectedFields);
  }

  @Test
  public void testRowkey() throws Exception {
    // entity with no row key mapping
    EntityMetadata metadata = EntityMetadata.getInstance();
    try {
      EntityInfo nokeyinfo = metadata.getInfo(Entities.NoKeyEntity.class);
      fail("EntityMetadata should have failed parsing NoKeyEntity due to missing HRowKey");
    }	catch (MappingException me) {
      assertTrue(me.getMessage().matches("Missing .* row key property"));
    }

    // duplicate row keys mapped -- only one is allowed
    try {
      EntityInfo dupekeyinfo = metadata.getInfo(Entities.DupeKeyEntity.class);
      fail("EntityMetadata should have failed parsing DupeKeyEntity due to duplicate HRowKey");
    } catch (MappingException me) {
      assertTrue(me.getMessage().matches("Duplicate mappings .* row key.*"));
    }

    // duplicate mappings for same field -- only one is allowed
    try {
      EntityInfo dupefieldinfo = metadata.getInfo(Entities.DupeFieldEntity.class);
      fail("EntityMetadata should have failed parsing DupeFieldEntity due to duplicate field mappings");
    } catch (MappingException me) {
      assertTrue(me.getMessage().matches("Duplicate mappings .* field.*"));
    }
  }

  @Test
  public void testIndexes() throws Exception {
    EntityMetadata metadata = EntityMetadata.getInstance();
    EntityInfo idxinfo = metadata.getInfo(Entities.IndexedEntity.class);
    IndexMapping idxmapping = idxinfo.getFirstPropertyIndex("stringProperty");
    assertNotNull(idxmapping);
    assertEquals("test_indexed-by_stringcol", idxmapping.getTableName());
  }
}
