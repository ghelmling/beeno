/**
 * 
 */
package meetup.beeno;

import meetup.beeno.mapping.EntityInfo;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * @author garyh
 *
 */
public interface QueryStrategy {

	ResultScanner createScanner() throws QueryException;
	
}
