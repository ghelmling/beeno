#
# Tests for parsing hbase entity mapping information
# from annotated java classes.

from jyunit.util import *

from meetup.beeno import EntityMetadata, HBaseException, MappingException
from meetup.beeno import TestEntities

import java.lang

class EntityTest(SimpleTest):
	def __init__(self, cls, tablename, fieldprops):
		self.name = "EntityTest[%s]" % str(cls.getName())
		self.entitycls = cls
		self.tablename = tablename
		self.fieldprops = fieldprops
		
	def run(self):
		metadata = EntityMetadata.getInstance()
		iteminfo = metadata.getInfo(self.entitycls)
		
		assertEquals(iteminfo.getTablename(), self.tablename)
	
		keyprop = iteminfo.getKeyProperty()
		assertNotNull(keyprop)
		assertEquals(keyprop.getName(), "id")
		
		
		for field, propname in self.fieldprops.items():
			prop = iteminfo.getFieldProperty(field)
			assertNotNull(prop)
			assertEquals(prop.getName(), propname)


def test_parsing():
	######### check the entity mapping test cases
	# simple success case
	expectedFields = {"props:stringcol": "stringProperty",
					  "props:intcol": "intProperty",
					  "props:floatcol": "floatProperty",
					  "props:doublecol": "doubleProperty",
					  "props:longcol": "longProperty"}
	simpletest = EntityTest(TestEntities.SimpleEntity, "test_simple", expectedFields)
	runTest(simpletest)

	############ check basic field mapping when indexes are present ########
	expectedFields = {"props:stringcol": "stringProperty",
					  "props:tscol": "timestamp"}
	idxtest = EntityTest(TestEntities.IndexedEntity, "test_indexed", expectedFields)
	runTest(idxtest)

def test_rowkey():	
	# entity with no row key mapping
	metadata = EntityMetadata.getInstance()
	try:
		nokeyinfo = metadata.getInfo(TestEntities.NoKeyEntity)
		fail("EntityMetadata should have failed parsing NoKeyEntity due to missing HRowKey")
	except MappingException, me:
		assertMatches(me.getMessage(), "Missing .* row key property")
	
	# duplicate row keys mapped -- only one is allowed
	try:
		dupekeyinfo = metadata.getInfo(TestEntities.DupeKeyEntity)
		fail("EntityMetadata should have failed parsing DupeKeyEntity due to duplicate HRowKey");
	except MappingException, me:
		assertMatches(me.getMessage(), "Duplicate mappings .* row key")
		
	# duplicate mappings for same field -- only one is allowed
	try:
		dupefieldinfo = metadata.getInfo(TestEntities.DupeFieldEntity)
		fail("EntityMetadata should have failed parsing DupeFieldEntity due to duplicate field mappings")
	except MappingException, me:
		assertMatches(me.getMessage(), "Duplicate mappings .* field")


def test_indexes():
	metadata = EntityMetadata.getInstance()
	idxinfo = metadata.getInfo(TestEntities.IndexedEntity)
	idxmapping = idxinfo.getFirstPropertyIndex("stringProperty")
	assertNotNull(idxmapping)
	assertEquals(idxmapping.getTableName(), "test_indexed-by_stringcol")
	
		
def run_test():
	test_parsing()
	test_rowkey()
	test_indexes()
	
	
if __name__ == "__main__":
	run_test()
