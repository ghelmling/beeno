package meetup.beeno;

import java.util.ArrayList;
import java.util.List;

import meetup.beeno.mapping.EntityInfo;
import meetup.beeno.mapping.EntityMetadata;
import meetup.beeno.mapping.MappingException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;

import org.apache.log4j.Logger;

/**
 * Provides a higher-level interface to retrieving stored HBase data than the HBase 
 * {@link org.apache.hadoop.hbase.client.Scanner} interface.  Generation of the internal Scanner
 * is based on the index information annotated in the entity class.
 * @author garyh
 *
 */
public class Query<T> {
	public static Logger log = Logger.getLogger(Query.class);
	
	protected EntityInfo entityInfo = null;
	protected QueryOpts opts = null;
	protected Criteria criteria = new Criteria();
	protected Criteria indexCriteria = new Criteria();
	protected EntityService<T> service = null;
	
	public Query(EntityService<T> service, Class<? extends T> entityClass) throws MappingException {
		this.service = service;
		this.entityInfo = EntityMetadata.getInstance().getInfo(entityClass);
		this.opts = new QueryOpts();
	}
	
	public QueryOpts getOptions() {
		return this.opts;
	}
	
	public void setOptions(QueryOpts opts) {
		this.opts = opts;
	}
	
	/**
	 * Sets the start key used for the query
	 * @param expression
	 * @return
	 */
	public Query<T> start(String key) {
		this.opts.setStartKey(key);
		return this;
	}
	
	/**
	 * Sets the start key used for the query
	 * @param expression
	 * @return
	 */
	public Query<T> start(byte[] keybytes) {
		this.opts.setStartKey(keybytes);
		return this;
	}

	/**
	 * Sets the timestamp portion to use in constructing 
	 * start keys for scanners
	 */
	public Query<T> startTime(Long time) {
		this.opts.setStartTime(time);
		return this;
	}

	/**
	 * Sets the stop row key to be used in the generated <code>Scan</code>
	 * instance.
	 */
	public Query<T> stop(String key) {
		this.opts.setStopKey(key);
		return this;
	}

	/**
	 * Sets the stop row key to be used in the generated <code>Scan</code>
	 * instance.
	 */
	public Query<T> stop(byte[] key) {
		this.opts.setStopKey(key);
		return this;
	}

	/**
	 * Defines an expression to be used in filtering query results
	 * @param expression
	 * @return
	 */
	public Query<T> where(Criteria.Expression expression) {
		this.criteria.add(expression);
		return this;
	}
	
	/**
	 * Specifies an indexed expression to use for the query
	 * @return
	 * @throws HBaseException
	 */
	public Query<T> using(Criteria.Expression expression) {
		this.indexCriteria.add(expression);
		return this;
	}
	
	/**
	 * Sets the maximum number of items to retrieve
	 * @return
	 * @throws HBaseException
	 */
	public Query<T> limit(int size) {
		this.opts.setPageSize(size);
		return this;
	}

	public List<T> execute() throws HBaseException {
		long t1 = System.nanoTime();
		List<T> entities = new ArrayList<T>();
		FilterList baseFilter = getCriteriaFilter(this.criteria.getExpressions());
		
		ResultScanner scanner = null;
		int processCnt = 0;
		try {
			scanner = getStrategy(baseFilter).createScanner();
			for (Result res : scanner) {
				processCnt++;
				T entity = this.service.createFromRow(res);
				if (entity != null)
					entities.add( entity );
			}
		}
		finally {
			// always clean up scanner resources
			if (scanner != null)
				try { scanner.close(); } catch (Exception e) { log.error("Error closing scanner", e); }
		}
		
		long t2 = System.nanoTime();
		log.info(String.format("HBASE TIMER: [%s] fetched %d records (processed %d) in %f msec.", 
				this.entityInfo.getEntityClass().getSimpleName(), entities.size(), processCnt, ((t2-t1)/1000000.0)));
		
		return entities;
	}

	public T executeSingle() throws HBaseException {
		// TODO: explicitly limit to 1 record in filter?
		List<T> results = execute();
		if (results != null && results.iterator().hasNext())
			return (T) results.iterator().next();
		
		return null;
	}
	
	protected QueryStrategy getStrategy(FilterList baseFilter) {
		QueryStrategy strat = null;
		if (!this.indexCriteria.isEmpty())
			strat = new ScanByIndex(this.entityInfo, this.opts, this.indexCriteria, baseFilter);
		else
			strat = new ScanNoIndex(this.entityInfo, this.opts, baseFilter);
		
		log.debug("Using strategy impl.: "+strat.getClass().getSimpleName());
		return strat;
	}

	protected FilterList getCriteriaFilter(List<Criteria.Expression> expressions) 
			throws HBaseException {
		FilterList filterset = new FilterList(FilterList.Operator.MUST_PASS_ALL, new ArrayList<Filter>());
		for (Criteria.Expression e : expressions) {
			filterset.addFilter( e.getFilter(this.entityInfo) );
		}

		if (this.opts.getPageSize() != -1 ) {
			// add on any query option filters
			if (log.isDebugEnabled())
				log.debug(String.format("Adding PageFilter size=%d", this.opts.getPageSize()));
			
			filterset.addFilter( new PageFilter(this.opts.getPageSize()) );
		}
		
		//return new WhileMatchRowFilter(filterset);
		return filterset;
	}
	
	
	protected void debugFilter(StringBuilder str, Filter filter, int depth) {
		if (depth == 0) {
			str.append("Using filter:\n");
		}
		else {
			for (int i=0; i<depth; i++)
				str.append('\t');
		}
		
		str.append('[').append(filter.getClass().getSimpleName());
		if (filter instanceof FilterList) {
			str.append('\n');
		}
		
		str.append(']').append('\n');
	}
}
