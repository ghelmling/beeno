#
# Tests for reading and writing entities to/from HBase
#

from jyunit.util import *

import java.lang
from java.util import ArrayList, HashSet, HashMap
from org.apache.hadoop.hbase.client import HTablePool
from meetup.beeno import EntityService, HBaseException
from meetup.beeno import TestEntities
from meetup.beeno.mapping import EntityMetadata, MappingException
from meetup.beeno.util import HUtil
from dbtest.hbase import HBaseContext

hc = HBaseContext()

def setup():
    hc.setUp()
    HUtil.setPool( HTablePool( hc.conf, 5 ) )
    # create a dummy HBase table for testing
    import db.hbase
    admin = db.hbase.Admin(hc.conf)

    if not admin.exists("test_simple"):
        admin.create("test_simple", {"props:": {}})
    if not admin.exists("test_complex"):
        admin.create("test_complex", {"props:": {db.hbase.VERSIONS: 10}, "extended:": {db.hbase.VERSIONS: 10}})

def teardown():
    try:
        # clean up the dummy table
        import db.hbase
        admin = db.hbase.Admin(hc.conf)

        if admin.exists("test_simple"):
            admin.disable("test_simple")
            admin.drop("test_simple")

        if admin.exists("test_complex"):
            admin.disable("test_complex")
            admin.drop("test_complex")
    finally:
        hc.tearDown()
        # hack to give server time to shutdown
        java.lang.Thread.sleep(10000)


def save_and_get():
	entity1 = TestEntities.SimpleEntity()
	entity1.setId("entity1")
	entity1.setStringProperty("all my words")
	entity1.setIntProperty(123)
	entity1.setFloatProperty(1.1)
	entity1.setDoubleProperty(123.456789)
	entity1.setLongProperty(444444444444)

	service = EntityService(TestEntities.SimpleEntity)
	service.save(entity1)

	entity2 = service.get("entity1")
	assertNotNull(entity2)
	assertEquals(entity2.getId(), entity1.getId())
	assertEquals(entity2.getStringProperty(), entity1.getStringProperty())
	assertEquals(entity2.getIntProperty(), entity1.getIntProperty())
	assertEquals(entity2.getFloatProperty(), entity1.getFloatProperty(), None, 0.0001)
	assertEquals(entity2.getDoubleProperty(), entity1.getDoubleProperty(), None, 0.0001)
	assertEquals(entity2.getLongProperty(), entity1.getLongProperty())

	# update the retrieved entity and check for the changes
	entity2.setStringProperty("new stuff")
	entity2.setDoubleProperty(9.8765432101)
	entity2.setIntProperty(-17)
	service.save(entity2)
	
	entity3 = service.get(entity2.getId())
	assertEquals(entity3.getId(), "entity1")
	assertEquals(entity3.getStringProperty(), "new stuff")
	assertEquals(entity3.getIntProperty(), -17)
	assertEquals(entity3.getFloatProperty(), 1.1, None, 0.0001)
	assertEquals(entity3.getDoubleProperty(), 9.8765432101, None, 0.0001)
	assertEquals(entity3.getLongProperty(), 444444444444)


def save_and_get_complex():
	'''Test saving entities containing mapped collection properties'''
	entity1 = TestEntities.ComplexEntity()
	entity1.setId("complex1")
	strings = ArrayList()
	strings.add("one")
	strings.add("two")
	entity1.setStringList(strings)
	ints = HashSet()
	ints.add(1)
	ints.add(2)
	entity1.setIntSet(ints)
	extended = HashMap()
	extended.put("prop1", "one")
	extended.put("prop2", "two")
	entity1.setExtendedProps(extended)
	
	service = EntityService(TestEntities.ComplexEntity)
	service.save(entity1)
	
	entity2 = service.get("complex1")
	assertNotNull(entity2)
	assertEquals(entity2.getId(), entity1.getId())
	assertTrue(entity2.getStringList().contains("one"))
	assertTrue(entity2.getStringList().contains("two"))
	assertTrue(entity2.getIntSet().contains(java.lang.Long(1)))
	assertTrue(entity2.getIntSet().contains(java.lang.Long(2)))
	assertNotNull(entity2.getExtendedProps())
	assertEquals(entity2.getExtendedProps().get("prop1"), "one")
	assertEquals(entity2.getExtendedProps().get("prop2"), "two")


def save_multiple():
	'''Test saving multiple entities as a batch'''
	entities = [ TestEntities.SimpleEntity("e1", "string1", 1, 1.1, 1.1, 1),
				 TestEntities.SimpleEntity("e2", "string2", 2, 2.2, 2.2, 2),
				 TestEntities.SimpleEntity("e3", "string3", 3, 3.3, 3.3, 3),
				 TestEntities.SimpleEntity("e4", "string4", 4, 4.4, 4.4, 4),
				 TestEntities.SimpleEntity("e5", "string5", 5, 5.5, 5.5, 5),
				 TestEntities.SimpleEntity("e6", "string6", 6, 6.6, 6.6, 6) ]
	
	srv = EntityService(TestEntities.SimpleEntity)
	srv.saveAll(entities)
	
	saved = []
	for e in entities:
		saved.append( srv.get(e.getId()) )
		
	assertEquals(len(entities), len(saved))
	for cnt in range(0, len(entities)):
		assertEquals(entities[cnt].getId(), saved[cnt].getId())
		assertEquals(entities[cnt].getStringProperty(), saved[cnt].getStringProperty())
		assertEquals(entities[cnt].getIntProperty(), saved[cnt].getIntProperty())
		assertEquals(entities[cnt].getFloatProperty(), saved[cnt].getFloatProperty(), None, 0.0001)
		assertEquals(entities[cnt].getDoubleProperty(), saved[cnt].getDoubleProperty(), None, 0.0001)
		assertEquals(entities[cnt].getLongProperty(), saved[cnt].getLongProperty())
 

def save_timeout():
	'''Test timeouts when saving recs'''
	newent = TestEntities.SimpleEntity("ne1", "string one", 1, 1.1, 1.1, 1);
	service = EntityService(TestEntities.SimpleEntity)
	try:
		# save with too short timeout
		success = service.save(newent, 1)
		assertFalse(success, "Expected save request to timeout")
	except HBaseException, e:
		fail("Save failed with unexpected exception")


def run_test():
	save_and_get()
	save_multiple()
	save_and_get_complex()
	#save_timeout()


if __name__ == "__main__":
	try:
		setup()
		run_test()
	finally:
		teardown()
	
