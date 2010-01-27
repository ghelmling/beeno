package meetup.beeno.mapping;

import java.beans.PropertyDescriptor;
import java.util.regex.Pattern;

import meetup.beeno.HProperty;

/**
 * Represents an annotated {@link HProperty} mapping of a JavaBean java.util.Collection type property
 * to multiple indexed columns in an HBase table.  The multiple values will be mapped to columns
 * based on the value index in the collection:
 * 		[column family]:[column name]_[index number]
 * 
 * Due to the mapping to multiple columns, these property types are not easily covered by HBase 
 * secondary indexes and should not be used as query criteria.
 * 
 * @author garyh
 *
 */
public class ListField extends FieldMapping {
	protected Pattern fieldRegex = null;
	public ListField(HProperty prop, PropertyDescriptor beanProperty) {
		super(prop, beanProperty);
		this.fieldRegex = Pattern.compile(this.family+":"+this.column+"_\\d+");
	}

	public boolean matches(String fieldname) { return this.fieldRegex.matcher(fieldname).matches(); }
}