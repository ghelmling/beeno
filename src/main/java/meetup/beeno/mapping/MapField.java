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

		this.fieldRegex = Pattern.compile(this.getColumn()+".+");
	}

  @Override
	public boolean matches(ColumnQualifier qualifier) {
    return this.getFamily().equals(qualifier.getFamily()) &&
        this.fieldRegex.matcher(qualifier.getQualifier()).matches();
  }
}