package meetup.beeno;

import static org.junit.Assert.*;

import java.util.List;

import junit.framework.Assert;

import meetup.beeno.Criteria;
import meetup.beeno.EntityService;
import meetup.beeno.HBaseException;
import meetup.beeno.Query;
import meetup.beeno.mapping.MappingException;
import meetup.beeno.util.HUtil;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/*
 * Commands to create the correspondign hbase schema in HBase Shell
 * create 'test_simple-by_photoId', '__idx__', 'props'
 * create 'test_simple', 'props'
 * Note: This is not required. The test case creates the corresponding tables automatically.
 */

public class BasicOrmPersistanceTest {

    private static HUtil _hUtil;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

	HBaseConfiguration conf = new HBaseConfiguration();
	conf.set("hbase.master", "localhost");
	HTablePool pool = new HTablePool(conf, 10);

	_hUtil = new HUtil();
	_hUtil.setPool(pool);

	HBaseAdmin admin = new HBaseAdmin(conf);

	// drop tables
	try {
	    admin.disableTable("test_simple-by_photoId");
	    admin.deleteTable("test_simple-by_photoId");
	} catch (TableNotFoundException e) {
	    // silently swallow
	}
	try {
	    admin.disableTable("test_simple");
	    admin.deleteTable("test_simple");
	} catch (TableNotFoundException e) {
	    // silently swallow
	}

	// create tables
	HTableDescriptor by_photoId_idx = new HTableDescriptor(
	"test_simple-by_photoId");
	by_photoId_idx.addFamily(new HColumnDescriptor("__idx__"));
	by_photoId_idx.addFamily(new HColumnDescriptor("props"));
	admin.createTable(by_photoId_idx);

	// create tables & index
	HTableDescriptor test_simple = new HTableDescriptor("test_simple");
	test_simple.addFamily(new HColumnDescriptor("props"));
	admin.createTable(test_simple);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testCreationAndCriteriaRetrieval() throws HBaseException {

	EntityService<SimpleEntity> service = EntityService
	.create(SimpleEntity.class);

	for (int i = 0; i < 10; i++) {
	    SimpleEntity se = new SimpleEntity();
	    se.setId("r:" + System.currentTimeMillis());
	    se.setStringProperty("superman");
	    se.setDoubleProperty(12.1111D);
	    se.setPhotoIdProperty("PHOTOID" + i);
	    service.save(se);
	}

	// query rows
	Query query = service.query().using(Criteria.eq("photoIdProperty", "PHOTOID5"));
	List<SimpleEntity> items = query.execute();
	for (SimpleEntity e : items) {
	    System.out.println("items: " + ((SimpleEntity) e).getId());
	}
	assertEquals(1, items.size());
    }

    @Test
    public void testCreationAndNonExistantCriteriaRetrieval() throws HBaseException {

	EntityService<SimpleEntity> service = EntityService.create(SimpleEntity.class);

	// query rows
	try{
	    Query query = service.query().using(Criteria.eq("IDONTEXISTFORSURE", "PHOTOID5"));
	    List<SimpleEntity> items = query.execute();
	    fail("Should have gotten a QueryException for the missing properties.");
	} catch(QueryException e){
	    //Sweet! Got our expected exception.
	}
    }

    @Test
    public void testCreationOfMixedExistingAndNonExistantCriteriaRetrieval() throws HBaseException {

	EntityService<SimpleEntity> service = EntityService.create(SimpleEntity.class);

	// query rows
	try{
	    Query query = service.query().using(Criteria.eq("IDONTEXISTFORSURE", "PHOTOID5")).where(Criteria.eq("photoIdProperty", "PHOTOID5"));
	    List<SimpleEntity> items = query.execute();
	    fail("Should have gotten a QueryException for the missing properties.");
	} catch(QueryException e){
	    //Sweet! Got our expected exception.
	}
    }


}
