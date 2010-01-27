/**
 * 
 */
package meetup.beeno;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import meetup.beeno.mapping.EntityInfo;
import meetup.beeno.mapping.IndexMapping;
import meetup.beeno.util.HUtil;
import meetup.beeno.util.PBUtil;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.log4j.Logger;

/**
 * @author garyh
 *
 */
public class ScanByIndex implements QueryStrategy {
	private static Logger log = Logger.getLogger(ScanByIndex.class);
	
	private final EntityInfo info;
	private final QueryOpts opts;
	private final Criteria indexConditions;
	private final Filter baseFilter;
	
	public ScanByIndex( EntityInfo info, QueryOpts opts, Criteria indexConditions, Filter baseFilter ) {
		this.info = info;
		this.opts = opts;
		this.indexConditions = indexConditions;
		this.baseFilter = baseFilter;
	}

	/* (non-Javadoc)
	 * @see com.meetup.db.hbase.QueryStrategy#createScanner(com.meetup.db.hbase.EntityMetadata.EntityInfo, org.apache.hadoop.hbase.filter.RowFilterInterface)
	 */
	@Override
	public ResultScanner createScanner()
			throws QueryException {
		
		ResultScanner scanner = null;

		try {
			HTable table = null;
			
			try {
				table = HUtil.getTable(info.getTablename());
				Criteria.PropertyExpression indexedExpr = selectIndexedExpression(info, indexConditions.getExpressions());
				if (indexedExpr != null) {
					log.debug("Using indexed expression: "+indexedExpr);
					IndexMapping idx = info.getFirstPropertyIndex(indexedExpr.getProperty());
					if (idx != null)
						log.debug("Using index table: "+idx.getTableName());
				
					byte[] startrow = getStartRow(opts, indexedExpr, idx);			
					//RowFilterInterface filter = addIndexFilters(baseFilter, startrow);
					log.debug("Using filter: "+baseFilter);
				
					long t1 = System.nanoTime();
					scanner = getIndexScanner(idx.getTableName(),
											  startrow, 
											  baseFilter, 
											  table,
											  null);
					long t2 = System.nanoTime();
					log.info(String.format("HBASE TIMER: created indexed scanner in %f msec.", ((t2-t1)/1000000.0)));
				}
				else {
					log.warn("Creating non-indexed scanner.  THIS MAY BE VERY SLOW!!!");
	
					byte[] startrow = getStartRow(opts, null, null);			
					log.debug("Using filter: "+baseFilter);
				
					long t1 = System.nanoTime();
					Scan scan = new Scan();
					if (startrow != null)
						scan.setStartRow(startrow);
					if (baseFilter != null)
						scan.setFilter(baseFilter);
					scanner = table.getScanner(scan);
					long t2 = System.nanoTime();
					log.info(String.format("HBASE TIMER: created scanner in %f msec.", ((t2-t1)/1000000.0)));
				}
			}
			finally {
				HUtil.releaseTable(table);
			}
		}
		catch (HBaseException he) {
			throw new QueryException(he);
		}
		catch (IOException ioe) {
			throw new QueryException(ioe);
		}

		return scanner;
	}
	
	
	protected ResultScanner getIndexScanner(String tablename, 
											byte[] startrow, 
											Filter filter, 
											HTable baseTable, 
											byte[][] families) 
		throws IOException {
		
		Scan idxScan = new Scan();
		idxScan.setStartRow(startrow);
		if (filter != null)
			idxScan.setFilter(filter);
		
		HTable idxTable = null;
		ResultScanner wrapper = null;
		try {
			idxTable = HUtil.getTable(tablename);
			ResultScanner idxScanner = idxTable.getScanner(idxScan);		
			wrapper = new IndexScannerWrapper(idxScanner, baseTable, families);
		}
		finally {
			if (idxTable != null)
				HUtil.releaseTable(idxTable);
		}
		
		return wrapper;
	}
	
	
	/**
	 * Tries to find which expression in the list will be able to use a secondary index table
	 * for the query.
	 * @param expressions
	 * @return
	 */
	protected Criteria.PropertyExpression selectIndexedExpression(EntityInfo info, 
																  List<Criteria.Expression> expressions) {
		// first look for a direct match expression
		for (Criteria.Expression e : expressions) {
			if (e instanceof Criteria.RequireExpression)
				e = ((Criteria.RequireExpression)e).getRequired();
			
			if (e instanceof Criteria.PropertyComparison) {
				Criteria.PropertyComparison propExpr = (Criteria.PropertyComparison)e;
				PropertyDescriptor prop = info.getProperty(propExpr.getProperty());
				if (prop != null && info.getFirstPropertyIndex(prop) != null)
					return propExpr;
			}
		}
		
		// fall back to the first indexed expression
		for (Criteria.Expression e : expressions) {
			if (e instanceof Criteria.RequireExpression)
				e = ((Criteria.RequireExpression)e).getRequired();
			
			if (e instanceof Criteria.PropertyExpression) {
				Criteria.PropertyExpression propExpr = (Criteria.PropertyExpression)e;
				PropertyDescriptor prop = info.getProperty(propExpr.getProperty());
				if (prop != null && info.getFirstPropertyIndex(prop) != null)
					return propExpr;
			}
		}
		
		return null;
	}
	
	
	protected byte[] getStartRow(QueryOpts opts, Criteria.PropertyExpression expr, IndexMapping idx) throws HBaseException {
		if (opts.getStartKey() != null) {
			return opts.getStartKey();
		}
		if (expr == null || idx == null) {
			return HConstants.EMPTY_START_ROW;
		}
		
		byte[] encValue = PBUtil.toBytes(expr.getValue()); 
		EntityIndexer generator = idx.getGenerator();
		
		return generator.createIndexKey(encValue, opts.getStartTime(), null);
	}
	
	protected Filter addIndexFilters(Filter baseFilter, byte[] startrow) {
		if (startrow != null) {
			List<Filter> orfilters = new ArrayList<Filter>(2);
			// allow the start row to pass regardless
			orfilters.add(new PageFilter(1));
			orfilters.add(baseFilter);
			FilterList filterset = new FilterList(FilterList.Operator.MUST_PASS_ONE, orfilters);
			return filterset;
		}

		// nothing else to add
		return baseFilter;
	}

	
	public static class IndexScannerWrapper implements ResultScanner {
		private final ResultScanner indexScanner;
		private final HTable baseTable;
		private final byte[][] baseFamilies;
		
		IndexScannerWrapper(ResultScanner indexScanner, HTable baseTable) {
			this(indexScanner, baseTable, null);
		}
		
		IndexScannerWrapper(ResultScanner indexScanner, HTable baseTable, byte[][] families) {
			this.indexScanner = indexScanner;
			this.baseTable = baseTable;
			this.baseFamilies = families;
		}
		
		@Override
		public Iterator<Result> iterator() {
			return new Iterator<Result>() {
				// store next item to support look ahead
				private Result next = null;
				
				public boolean hasNext() {
					if (next == null) {
						try {
							next = IndexScannerWrapper.this.next();
							return next != null;
						}
						catch (IOException ioe) {
							throw new RuntimeException(ioe);
						}
					}
					
					return true;
				}
				
				public Result next() {
					// use hasNext to advance
					if (!hasNext())
						return null;
					
					// clear next pointer for next operation
					Result tmp = next;
					next = null;
					return tmp;
				}
				
				/*
				 * Not supported
				 */
				public void remove() {
					throw new UnsupportedOperationException("Not supported");
				}
			};
		}

		@Override
		public void close() {
			this.indexScanner.close();
			HUtil.releaseTable(this.baseTable);
		}

		@Override
		/**
		 * Advances the index scanner and reads the next record from the underlying table
		 */
		public Result next() throws IOException {
			Result idxRow = this.indexScanner.next();
			if (idxRow != null && !idxRow.isEmpty()) {
				byte[] rowkey = idxRow.getValue(EntityIndexer.INDEX_FAMILY, EntityIndexer.INDEX_KEY_COLUMN);
				if (rowkey != null && rowkey.length > 0) {
					Get get = new Get(rowkey);
					if (this.baseFamilies != null)
						for (byte[] fam : this.baseFamilies)
							get.addFamily(fam);
					
					return this.baseTable.get(get);
				}
				else {
					if (log.isDebugEnabled())
						log.debug("No base record found for index key");
				}
			}
			
			return null;
		}

		@Override
		public Result[] next( int count ) throws IOException {
			ArrayList<Result> results = new ArrayList<Result>(count);
			int iter = 0;
			while (iter < count) {
				iter++;
				Result next = next();
				if (next == null)
					break;
				
				results.add(next);
			}
			
			return results.toArray(new Result[0]);
		}
		
	}
}
