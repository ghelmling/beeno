package meetup.beeno.mapping;

import java.beans.PropertyDescriptor;
import java.util.regex.Pattern;

import meetup.beeno.HProperty;

/**
 * Represents an annotated {@link HProperty} mapping of a JavaBean java.util.Map type property
 * to multiple columns (one per Map key) in an HBase table.  The Map entries will be mapped to columns
 * in the specified column family, using the convention:
 * 		[column family]:[entry key]
 * 
 * Like the ListField mapping, these property types cannot easily be indexed using HBase secondary indexes, 
 * due to the dynamic nature of the column names.  So these properties should not be used as query criteria.
 * 
 * @author garyh
 *
 */
public class MapField extends FieldMapping {
	protected Pattern fieldRegex = null;
	public MapField(HProperty prop, PropertyDescriptor beanProperty) {
		super(prop, beanProperty);
		if (this.column == null || this.column.equals("*"))
			this.column = "";
		
		this.fieldRegex = Pattern.compile(this.family+":"+this.column+".+");
		// fieldname should ignore wildcard pattern
		this.fieldname = this.family + ":";
	}

	public boolean matches(String fieldname) { return this.fieldRegex.matcher(fieldname).matches(); }
}