package meetup.beeno;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

import meetup.beeno.util.HUtil;

public class TestPool {

	private String table;
	private int size;
	
	public TestPool(String table, int size) {
		this.table = table;
		this.size = size;
	}
	
	
	public void runPool() {
		List<HTableInterface> held = new LinkedList<HTableInterface>();
		
		for (int i=0; i<this.size; i++) {
			HTableInterface t = HUtil.getTable(table);
			held.add( t );
			Get g = new Get(Bytes.toBytes("4679998"));
			try {
				t.get(g);
			}
			catch (Exception e) {
				System.err.println("Get failed: ");
				e.printStackTrace();
			}
		}
		
		System.out.printf("Fetched %d table instances\n", held.size());
		
		for (HTableInterface t : held)
			HUtil.releaseTable(t);
		
		held = null;
		System.out.println("Released all held tables");
	}
	
	public void runNew() {
		List<HTable> held = new LinkedList<HTable>();
		
		for (int i=0; i<this.size; i++)
			try {
				HTable t = new HTable(table);
				held.add( t );
				Get g = new Get(Bytes.toBytes("4679998"));
				t.get(g);
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
		
		System.out.printf("Created %d table instances\n", held.size());
		
		for (HTable t : held)
			try {
				t.close();
			}
			catch (IOException ioe) {
				ioe.printStackTrace();
			}
			
		held = null;
		System.out.println("Released all held tables");		
	}
	
	/**
	 * @param args
	 */
	public static void main( String[] args ) {
		TestPool test = new TestPool("MemberFeedIndex", 20);
		test.runPool();
	}

}
