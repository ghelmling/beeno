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
	protected ColumnQualifier qualifier = null;
	protected PropertyDescriptor beanProperty = null;

	public FieldMapping(HProperty prop, PropertyDescriptor beanProperty) {
		this.qualifier = new ColumnQualifier(prop.family(), prop.name());
		this.beanProperty = beanProperty;
	}

  public boolean matches(ColumnQualifier qualifier) {
    return this.qualifier.equals(qualifier);
  }

	public PropertyDescriptor getBeanProperty() {
    return this.beanProperty;
  }

	public String getFamily() {
    return this.qualifier.getFamily();
  }

	public String getColumn() {
    return this.qualifier.getQualifier();
  }

  public ColumnQualifier getQualifier() {
    return this.qualifier;
  }

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