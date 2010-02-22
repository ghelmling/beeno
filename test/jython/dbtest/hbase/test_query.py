#
# Tests out usage of the general HBase query interface
#

from jyunit.util import *

import java.lang

import db.hbase
from org.apache.hadoop.hbase.client import HTablePool
from meetup.beeno import EntityService, Query, Criteria, HBaseException
from meetup.beeno.util import HUtil
from meetup.beeno import TestEntities
from meetup.beeno.mapping import EntityMetadata
from dbtest.hbase import HBaseContext

hc = HBaseContext()

def setup():
    hc.setUp()
    HUtil.setPool( HTablePool( hc.conf, 5 ) )
    # create a dummy HBase table for testing
    import db.hbase
    admin = db.hbase.Admin(hc.conf)

    if not admin.exists("test_indexed"):
        admin.create("test_indexed", {"props:": {}})	
    if not admin.exists("test_indexed-by_intcol"):
        admin.create("test_indexed-by_intcol", {"props:": {}, "__idx__:": {}})
    if not admin.exists("test_indexed-by_stringcol"):
        admin.create("test_indexed-by_stringcol", {"props:": {}, "__idx__:": {}})

    srv = EntityService(TestEntities.IndexedEntity)
    now = java.lang.System.currentTimeMillis()

    srv.save( TestEntities.IndexedEntity("e1", "duck", 1, now - 100) )
    srv.save( TestEntities.IndexedEntity("e2", "duck", 2, now - 80) )
    srv.save( TestEntities.IndexedEntity("e3", "duck", 2, now - 60) )
    srv.save( TestEntities.IndexedEntity("e4", "goose", 2, now - 40) )


def teardown():
    try:
        pass
        # clean up the dummy table
        import db.hbase
        admin = db.hbase.Admin(hc.conf)

        if admin.exists("test_indexed"):
            admin.drop("test_indexed")
        if admin.exists("test_indexed-by_intcol"):
            admin.drop("test_indexed-by_intcol")
        if admin.exists("test_indexed-by_stringcol"):
            admin.drop("test_indexed-by_stringcol")
    finally:
        hc.tearDown()
        # hack to give server time to shutdown
        java.lang.Thread.sleep(10000)


def query_by_string():
    srv = EntityService(TestEntities.IndexedEntity)
    # test indexing of a value with multiple entries
    q = srv.query()
    q.using( Criteria.eq( "stringProperty", java.lang.String('duck') ) )
    matches = q.execute()

    assertEquals( len(matches), 3 )
    assertEquals( matches[0].getId(), 'e1' )
    assertEquals( matches[0].getStringProperty(), 'duck' )
    assertEquals( matches[1].getId(), 'e2' )
    assertEquals( matches[1].getStringProperty(), 'duck' )
    assertEquals( matches[2].getId(), 'e3' )
    assertEquals( matches[2].getStringProperty(), 'duck' )

    q = srv.query()
    q.using( Criteria.eq( "stringProperty", java.lang.String('goose') ) )
    matches = q.execute()
    assertEquals( len(matches), 1 )
    assertEquals( matches[0].getId(), 'e4' )
    assertEquals( matches[0].getStringProperty(), 'goose' )


def query_by_int():
    srv = EntityService(TestEntities.IndexedEntity)
    # test indexing of integer values
    q = srv.query()
    q.using( Criteria.eq( "intKey", java.lang.Integer(2) ) )
    matches = q.execute()
    print matches
    assertEquals( len(matches), 3 )
    assertEquals( matches[0].getId(), 'e4', "Indexed entries should be in reverse timestamp order" )
    assertEquals( matches[0].getIntKey(), 2 )
    assertEquals( matches[1].getId(), 'e3', "Indexed entries should be in reverse timestamp order" )
    assertEquals( matches[1].getIntKey(), 2 )
    assertEquals( matches[2].getId(), 'e2', "Indexed entries should be in reverse timestamp order" )
    assertEquals( matches[2].getIntKey(), 2 )

    q = srv.query()
    q.using( Criteria.eq( "intKey", java.lang.Integer(1) ) )
    matches = q.execute()
    assertEquals( len(matches), 1 )
    assertEquals( matches[0].getId(), 'e1' )
    assertEquals( matches[0].getIntKey(), 1 )


def run_test():
    query_by_string()
    query_by_int()


if __name__ == '__main__':
    try:
        setup()
        run_test()
    finally:
        teardown()
