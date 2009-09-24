package meetup.beeno;

import java.io.IOException;

import meetup.beeno.util.HUtil;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

public class ScanNoIndex implements QueryStrategy {
	private static Logger log = Logger.getLogger(ScanNoIndex.class);
	
	public ScanNoIndex() {
	}
	
	@Override
	public ResultScanner createScanner( EntityMetadata.EntityInfo entityInfo, QueryOpts opts, Filter baseFilter )
			throws QueryException {
		ResultScanner scanner = null;

		Scan scan = new Scan();
		HTable table = null;
		try {
			table = HUtil.getTable(entityInfo.getTablename());
	
			scan.setFilter(baseFilter);
			log.debug("Using filter: "+baseFilter);

			if (opts.getStartKey() != null)
				scan.setStartRow(opts.getStartKey());
			
		
			long t1 = System.nanoTime();
			scanner = table.getScanner(scan);
			long t2 = System.nanoTime();
			log.info(String.format("HBASE TIMER: created scanner in %f msec.", ((t2-t1)/1000000.0)));
		} catch (IOException ioe) {
			throw new QueryException(ioe);
		}
		finally {
			HUtil.releaseTable(table);
		}
	
		return scanner;
	}
}
