/**
 * 
 */
package meetup.beeno;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * @author garyh
 *
 */
public interface QueryStrategy {

	ResultScanner createScanner(EntityMetadata.EntityInfo info, QueryOpts opts, Filter baseFilter) 
		throws QueryException;
	
}
