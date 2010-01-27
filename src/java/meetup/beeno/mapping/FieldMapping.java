package meetup.beeno.mapping;

import java.beans.PropertyDescriptor;
import java.util.Collection;
import java.util.Map;

import meetup.beeno.HProperty;

/**
 * Stores an annotated {@link HProperty} mapping of a JavaBean property
 * in the entity class to an HBase table column.  This maps a single Java object
 * instance to a single column.
 */
public class FieldMapping {
	protected String family = null;
	protected String column = null;
	protected String fieldname = null;
	protected PropertyDescriptor beanProperty = null;
	public FieldMapping(HProperty prop, PropertyDescriptor beanProperty) {
		this.family = prop.family();
		this.column = prop.name();
		this.fieldname = this.family+":"+this.column;
		this.beanProperty = beanProperty;
	}

	public boolean matches(String fieldname) { return this.fieldname.equals(fieldname); }
	public PropertyDescriptor getBeanProperty() { return this.beanProperty; }
	public String getFamily() { return this.family; }
	public String getColumn() { return this.column; }
	public String getFieldName() { return this.fieldname; }

	public static FieldMapping get(HProperty prop, PropertyDescriptor beanProperty) {
		if (Map.class.isAssignableFrom(beanProperty.getPropertyType())) {
			return new MapField(prop, beanProperty);
		}
		else if (Collection.class.isAssignableFrom(beanProperty.getPropertyType())) {
			return new ListField(prop, beanProperty);
		}

		return new FieldMapping(prop, beanProperty);
	}
}