package meetup.beeno;

import java.util.ArrayList;
import java.util.List;

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
	
	protected EntityMetadata.EntityInfo entityInfo = null;
	protected QueryOpts opts = null;
	protected EntityService<T> service = null;
	
	public Query(EntityService<T> service, Class entityClass) throws MappingException {
		this(service, entityClass, null);
	}

	public Query(EntityService<T> service, Class entityClass, QueryOpts opts) 
		throws MappingException {
		this.service = service;
		this.entityInfo = EntityMetadata.getInstance().getInfo(entityClass);
		this.opts = opts;
		if (this.opts == null)
			this.opts = new QueryOpts();
	}
	
	public QueryOpts getOptions() {
		return this.opts;
	}
	
	public void setOptions(QueryOpts opts) {
		this.opts = opts;
	}
	
	public Query add(Criteria.Expression expression) {
		this.opts.addCriteria(expression);
		return this;
	}
	
	public List<T> execute() throws HBaseException {
		long t1 = System.nanoTime();
		List entities = new ArrayList();
		FilterList baseFilter = getCriteriaFilter(this.opts.getCriteria().getExpressions());
		
		ResultScanner scanner = null;
		int processCnt = 0;
		try {
			scanner = getStrategy().createScanner(this.entityInfo, this.opts, baseFilter);
			for (Result res : scanner) {
				processCnt++;
				// re-apply the basic criteria filter to exclude the start row, if it doesn't
				// match but was allowed for indexing purposes
				// FIXME: GH - hack to work around secondary index filtering issues
				/*
				if (baseFilter.filterRowKey(res.getRow()) || baseFilter.filterRow(res)) {
					if (log.isDebugEnabled()) 
						log.debug("Skipping row: "+Bytes.toString(res.getRow()));
					continue;
				}
				*/
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
		List results = execute();
		if (results != null && results.iterator().hasNext())
			return (T) results.iterator().next();
		
		return null;
	}
	
	protected QueryStrategy getStrategy() {
		QueryStrategy strat = null;
		if (this.opts.shouldUseIndex())
			strat = new ScanByIndex();
		else
			strat = new ScanNoIndex();
		
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
